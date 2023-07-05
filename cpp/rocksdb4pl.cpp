/*  Part of SWI-Prolog

    Author:        Jan Wielemaker
    E-mail:        J.Wielemaker@vu.nl
    WWW:           http://www.swi-prolog.org
    Copyright (c)  2022, VU University Amsterdam
			 SWI-Prolog Solutions b.v.
    All rights reserved.

    Redistribution and use in source and binary forms, with or without
    modification, are permitted provided that the following conditions
    are met:

    1. Redistributions of source code must retain the above copyright
       notice, this list of conditions and the following disclaimer.

    2. Redistributions in binary form must reproduce the above copyright
       notice, this list of conditions and the following disclaimer in
       the documentation and/or other materials provided with the
       distribution.

    THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
    "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
    LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS
    FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE
    COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
    INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
    BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
    LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
    CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
    LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN
    ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
    POSSIBILITY OF SUCH DAMAGE.
*/

#include <cassert>
#include <mutex>
#include <rocksdb/db.h>
#include <rocksdb/env.h>
#include <rocksdb/write_batch.h>
#include <rocksdb/merge_operator.h>
#include <rocksdb/statistics.h>
#define PL_ARITY_AS_SIZE 1
#include <SWI-Stream.h>
#include <SWI-Prolog.h>
#include <SWI-cpp2.h>

#include <SWI-cpp2.cpp> // This could be put in a separate file


[[nodiscard]] static PlAtom rocks_get_alias(PlAtom name);
[[nodiscard]] static PlAtom rocks_get_alias_inside_lock(PlAtom name);
static void rocks_add_alias(PlAtom name, PlAtom symbol);
static void rocks_add_alias_inside_lock(PlAtom name, PlAtom symbol);
static void rocks_unalias(PlAtom name);
static void rocks_unalias_inside_lock(PlAtom name);

		 /*******************************
		 *	       SYMBOL		*
		 *******************************/

enum blob_type
{ BLOB_ATOM = 0,			/* UTF-8 string as atom */
  BLOB_STRING,				/* UTF-8 string as string */
  BLOB_BINARY,				/* Byte string as string */
  BLOB_INT32,				/* 32-bit native integer */
  BLOB_INT64,				/* 64-bit native integer */
  BLOB_FLOAT32,				/* 32-bit IEEE float */
  BLOB_FLOAT64,				/* 64-bit IEEE double */
  BLOB_TERM				/* Arbitrary term */
};

static const char* blob_type_char[] =
{ "atom",
  "string",
  "binary",
  "int32",
  "int64",
  "float32",
  "float64",
  "term"
};

enum merger_t
{ MERGE_NONE = 0,
  MERGE_LIST,
  MERGE_SET
};

static const char* merge_t_char[] =
{ "none",
  "list",
  "set"
};

struct dbref_type_kv
{ blob_type key;
  blob_type value;
};


struct dbref;

static PL_blob_t rocks_blob = PL_BLOB_DEFINITION(dbref, "rocksdb");

struct dbref : public PlBlob
{
  rocksdb::DB	*db = nullptr;			    // DB handle
  PlAtom	 pathname = PlAtom(PlAtom::null);   // DB's absolute file name (for debugging)
  PlAtom	 name     = PlAtom(PlAtom::null);   // alias name (can be PlAtom::null)
  merger_t	 builtin_merger = MERGE_NONE;	    // C++ Merger
  PlRecord	 merger = PlRecord(PlRecord::null); // merge option
  dbref_type_kv  type = {			    // key and value types
			  .key   = BLOB_ATOM,
			  .value = BLOB_ATOM};

  explicit dbref() :
    PlBlob(&rocks_blob) { }

  PL_BLOB_SIZE

  bool write_fields(IOSTREAM *s, int flags) const override
  { if ( pathname.not_null() )
      if ( !Sfprintf(s, ",path=") ||
           !pathname.write(s, flags) )
        return false;
    if ( name.not_null() )
      if ( !Sfprintf(s, ",alias=") ||
           !name.write(s, flags) )
        return false;
    if (builtin_merger != MERGE_NONE)
      if ( !Sfprintf(s, ",builtin_merger=%s", merge_t_char[builtin_merger]) )
        return false;
    if ( merger.not_null() )
    { auto m(merger.term());
      if ( m.not_null() )
      { if ( !Sfprintf(s, ",merger=") ||
             !m.write(s, 1200, flags) )
          return false;
      }
    }
    if ( !db )
      if ( !Sfprintf(s, ",CLOSED") )
        return false;
    return Sfprintf(s, ",key=%s,value=%s)", blob_type_char[type.key], blob_type_char[type.value]);
  }

  int compare_fields(const PlBlob* _b_data) const override
  { // dynamic_cast is safer, but slower:
    auto b_data = static_cast<const dbref*>(_b_data);
    int c_pathname = PlTerm_atom(pathname).compare(PlTerm_atom(b_data->pathname));
    if ( c_pathname != 0 )
      return c_pathname;
    return 0;
  }

  ~dbref()
  { // See also predicate rocks_close/1
    if ( name.not_null() )
    { rocks_unalias(name);
      name.unregister_ref();
    }
    if ( merger.not_null() )
      merger.erase();
    if ( pathname.not_null() )
      pathname.unregister_ref();
    if ( db )
    { (void)db->Close(); // TODO: print warning on failure?
      delete db;
    }
  }
};


		 /*******************************
		 *	      ALIAS		*
		 *******************************/

std::mutex rocksdb4pl_alias_lock; // global
// TODO: Define the necessary operators for PlAtom, so that it can be
//       the key instead of atom_t.
static std::map<atom_t, PlAtom> alias_entries;

[[nodiscard]]
static PlAtom
rocks_get_alias_inside_lock(PlAtom name)
{ const auto lookup = alias_entries.find(name.C_);
  if ( lookup == alias_entries.end() )
    return PlAtom(PlAtom::null);
  else
    return lookup->second;
}

[[nodiscard]]
static PlAtom
rocks_get_alias(PlAtom name)
{ std::lock_guard<std::mutex> alias_lock_(rocksdb4pl_alias_lock);
  return rocks_get_alias_inside_lock(name);
}

static void
rocks_add_alias_inside_lock(PlAtom name, PlAtom symbol)
{ const auto lookup = rocks_get_alias_inside_lock(name);
  if ( lookup.is_null() )
  { alias_entries.insert(std::make_pair(name.C_, symbol));
    name.register_ref();
    symbol.register_ref();
  } else if ( lookup != symbol )
  { throw PlPermissionError("alias", "rocksdb", PlTerm_atom(name));
  }
}

static void
rocks_add_alias(PlAtom name, PlAtom symbol)
{ std::lock_guard<std::mutex> alias_lock_(rocksdb4pl_alias_lock);
  rocks_add_alias_inside_lock(name, symbol);
}

static void
rocks_unalias_inside_lock(PlAtom name)
{ auto lookup = alias_entries.find(name.C_);
  if ( lookup == alias_entries.end() )
    return;
  // TODO: As an alternative to removing the entry, leave it in place
  //       (with db==nullptr showing that it's been closed; or with
  //       the value as PlAtom::null), so that rocks_close/1 can
  //       distinguish an alias lookup that should throw a
  //       PlExistenceError because it's never been opened.
  name.unregister_ref();
  lookup->second.unregister_ref();  // alias_entries[name].unregister_ref()
  alias_entries.erase(lookup);
}

static void
rocks_unalias(PlAtom name)
{ std::lock_guard<std::mutex> alias_lock_(rocksdb4pl_alias_lock);
  rocks_unalias_inside_lock(name);
}


		 /*******************************
		 *	 SYMBOL REFERENCES	*
		 *******************************/


/* - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
GC a rocks dbref blob from the atom garbage collector.
- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - */


[[nodiscard]]
static dbref *
symbol_dbref(PlAtom symbol)
{ return PlBlobV<dbref>::cast(symbol);
}


[[nodiscard]]
static dbref *
get_rocks(PlTerm t, bool throw_if_closed=true)
{ PlAtom a(t.as_atom()); // Throws type error if not an atom

  auto ref = symbol_dbref(a);
  if ( !ref )
  { a = rocks_get_alias(a);
    if ( a.not_null() )
      ref = symbol_dbref(a);
  }
  if ( throw_if_closed &&
       ( !ref || !ref->db ) )
    throw PlExistenceError("rocksdb", t);

  return ref;
}


		 /*******************************
		 *	      UTIL		*
		 *******************************/

static PlException
RocksError(const rocksdb::Status& status, const dbref *ref)
{ if ( ref )
    return PlGeneralError(PlCompound("rocks_error",
				     PlTermv(PlTerm_atom(status.ToString()),
					     ref->symbol_term())));
  return PlGeneralError(PlCompound("rocks_error",
				   PlTermv(PlTerm_atom(status.ToString()))));
}


static bool
ok(const rocksdb::Status& status, const dbref *ref)
{ if ( status.ok() )
    return true;
  if ( status.IsNotFound() )
    return false;
  throw RocksError(status, ref);
}

static void
ok_or_throw_fail(const rocksdb::Status& status, const dbref *ref)
{ PlCheckFail(ok(status, ref));
}


class PlSlice
{
public:
  explicit PlSlice()
    : slice_() { }
  explicit PlSlice(const char *d, size_t n)
    : slice_(d, n) { }
  explicit PlSlice(const std::string& s)
    : slice_(s) { }

  const char* data() const { return slice_.data(); }
  size_t size() const { return slice_.size(); }
  std::string ToString(bool hex = false) const { return slice_.ToString(hex); }
  const rocksdb::Slice& slice() const { return slice_; }

  virtual ~PlSlice() = default;

protected:
  rocksdb::Slice slice_;
};

template<typename T>
class PlSliceNumber : public PlSlice
{
public:
  explicit PlSliceNumber<T>(T v)
    : PlSlice(reinterpret_cast<const char *>(&v_), sizeof v_),
      v_(v) { }

  virtual ~PlSliceNumber<T>() = default;

protected:
  T v_; // backing store for rocksdb::slice
};

class PlSliceStr : public PlSlice
{
public:
  explicit PlSliceStr(const std::string& s)
    : v_(s)
  { // Assign slice after setting up v_ because Slice(v_) uses both
    // the address of v_ and the size; if we used the initializer
    // list, the fields are done in the order of their declaration.
    slice_ = rocksdb::Slice(v_);
  }

  virtual ~PlSliceStr() = default;

protected:
  std::string v_; // backing store for rocksdb::slice
};


#define CVT_IN	(CVT_ATOM|CVT_STRING|CVT_LIST)

[[nodiscard]]
static std::unique_ptr<PlSlice>
get_slice(PlTerm t, blob_type type)
{ switch ( type )
  { case BLOB_ATOM:
    case BLOB_STRING:
      return std::make_unique<PlSliceStr>(t.get_nchars(CVT_IN|CVT_EXCEPTION|REP_UTF8));
    case BLOB_BINARY:
      return std::make_unique<PlSliceStr>(t.get_nchars(CVT_IN|CVT_EXCEPTION));
    case BLOB_INT32:
    { int32_t v;
      t.integer(&v);
      return std::make_unique<PlSliceNumber<int32_t>>(v);
    }
    case BLOB_INT64:
    { int64_t v;
      t.integer(&v);
      return std::make_unique<PlSliceNumber<int64_t>>(v);
    }
    case BLOB_FLOAT32:
      return std::make_unique<PlSliceNumber<float>>(static_cast<float>(t.as_float()));
    case BLOB_FLOAT64:
      return std::make_unique<PlSliceNumber<double>>(t.as_double());
    case BLOB_TERM:
    { PlRecordExternalCopy e(t); // declared here so that it stays in scope for return
      return std::make_unique<PlSliceStr>(e.data());
    }
    default:
      assert(0);
  }
}


[[nodiscard]]
static bool
unify(PlTerm t, const rocksdb::Slice& s, blob_type type)
{ switch ( type )
  { case BLOB_ATOM:
      return t.unify_chars(PL_ATOM|REP_UTF8, s.size_, s.data_);
    case BLOB_STRING:
      return t.unify_chars(PL_STRING|REP_UTF8, s.size_, s.data_);
    case BLOB_BINARY:
      return t.unify_chars(PL_STRING|REP_ISO_LATIN_1, s.size_, s.data_);
    case BLOB_INT32:
    { int i;
      memcpy(&i, s.data_, sizeof i); // Unaligned i=*reinterpret_cast<int>(s.data_)
      return t.unify_integer(i);
    }
    case BLOB_INT64:
    { int64_t i;
      memcpy(&i, s.data_, sizeof i);
      return t.unify_integer(i);
    }
    case BLOB_FLOAT32:
    { float f;
      memcpy(&f, s.data_, sizeof f);
      return t.unify_float(f);
    }
    case BLOB_FLOAT64:
    { double f;
      memcpy(&f, s.data_, sizeof f);
      return t.unify_float(f);
    }
    case BLOB_TERM:
    { PlTerm_var tmp;
      Plx_recorded_external(s.data_, tmp.C_);
      return tmp.unify_term(t);
    }
    default:
      assert(0);
      return false;
  }
}

[[nodiscard]]
static bool
unify(PlTerm t, const rocksdb::Slice *s, blob_type type)
{ if ( s )
    return unify(t, *s, type);

  switch ( type )
  { case BLOB_ATOM:
    { static const PlAtom ATOM_("");
      return t.unify_atom(ATOM_);
    }
    case BLOB_STRING:
    case BLOB_BINARY:
      return t.unify_chars(PL_STRING, 0, "");
    case BLOB_INT32:
    case BLOB_INT64:
      return t.unify_integer(0);
    case BLOB_FLOAT32:
    case BLOB_FLOAT64:
      return t.unify_float(0.0);
    case BLOB_TERM:
      return t.unify_nil();
    default:
      assert(0);
      return false;
  }
}

[[nodiscard]]
static bool
unify(PlTerm t, const std::string& s, blob_type type)
{ rocksdb::Slice sl(s);

  return unify(t, sl, type);
}

[[nodiscard]]
static bool
unify_value(PlTerm t, const rocksdb::Slice& s, merger_t merge, blob_type type)
{ if ( merge == MERGE_NONE )
   return unify(t, s, type);

  PlTerm_tail list(t);
  PlTerm_var tmp;
  const char *data = s.data();
  const char *end  = data+s.size();

  while ( data < end )
  { switch ( type )
    { case BLOB_INT32:
      { int i;
	memcpy(&i, data, sizeof i);
	data += sizeof i;
	Plx_put_integer(tmp.C_, i);
      }
      break;
      case BLOB_INT64:
      { int64_t i;
	memcpy(&i, data, sizeof i);
	data += sizeof i;
	Plx_put_int64(tmp.C_, i);
      }
      break;
      case BLOB_FLOAT32:
      { float i;
	memcpy(&i, data, sizeof i);
	data += sizeof i;
	Plx_put_float(tmp.C_, i);
      }
      break;
      case BLOB_FLOAT64:
      { double i;
	memcpy(&i, data, sizeof i);
	data += sizeof i;
	Plx_put_float(tmp.C_, i);
      }
      break;
      default:
	assert(0);
	return false;
    }

    if ( !list.append(tmp) )
      return false;
  }

  return list.close();
}

[[nodiscard]]
static bool
unify_value(PlTerm t, const std::string& s, merger_t merge, blob_type type)
{ rocksdb::Slice sl(s);

  return unify_value(t, sl, merge, type);
}


		 /*******************************
		 *	       MERGER		*
		 *******************************/

[[nodiscard]]
static bool
log_exception(rocksdb::Logger* logger)
{ PlTerm_term_t ex(Plx_exception(0));

  Log(logger, "%s", ex.as_string(PlEncoding::UTF8).c_str());
  return false; // For convenience, allowing: return log_exception(logger);
}

class engine
{
private:
  int tid = 0;

public:
  engine()
  { if ( Plx_thread_self() == -1 )
    { if ( (tid=Plx_thread_attach_engine(nullptr)) < 0 )
      { PlTerm_term_t ex(Plx_exception(0));
	if ( ex.not_null() )
	  throw PlException(ex);
	else
	  throw PlResourceError("memory");
      }
    }
  }

  ~engine()
  { if ( tid > 0 )
      Plx_thread_destroy_engine();
  }
};

[[nodiscard]]
static bool
call_merger(const dbref_type_kv& type, PlTermv av, std::string* new_value,
	    rocksdb::Logger* logger)
{ static PlPredicate pred_call6(PlFunctor("call", 6), PlModule("system"));

  try
  { PlQuery q(pred_call6, av);
    if ( q.next_solution() )
    { const auto answer = get_slice(av[5], type.value);
      new_value->assign(answer->data(), answer->size());
      return true;
    } else
    { Log(logger, "merger failed");
      return false;
    }
  } catch(PlException& ex)
  { Log(logger, "%s", ex.as_string(PlEncoding::UTF8).c_str());
    throw;
  }
}


class PrologMergeOperator : public rocksdb::MergeOperator
{
private:
  dbref_type_kv type;
  merger_t      builtin_merger;
  PlRecord      merger;

public:
  explicit PrologMergeOperator(const dbref& ref)
    : type(ref.type), builtin_merger(ref.builtin_merger), merger(ref.merger.duplicate())
  { }

  ~PrologMergeOperator()
  { merger.erase();
  }

  virtual bool
  FullMerge(const rocksdb::Slice& key,
	    const rocksdb::Slice* existing_value,
	    const std::deque<std::string>& operand_list,
	    std::string* new_value,
	    rocksdb::Logger* logger) const override
  { engine e_ctxt;
    PlTermv av(6);
    PlTerm_tail list(av[4]);
    PlTerm_var tmp;
    static const PlAtom ATOM_full("full");

    for (const auto& value : operand_list)
    { Plx_put_variable(tmp.C_);
      if ( !unify(tmp, value, type.value) ||
	   !list.append(tmp) )
	return false;
    }
    if ( !list.close() )
      return false;

    if ( av[0].unify_term(merger.term()) &&
	 av[1].unify_atom(ATOM_full) &&
	 unify(av[2], key, type.key) &&
	 unify(av[3], existing_value, type.value) )
      return call_merger(type, av, new_value, logger);
    else
      return log_exception(logger);
  }

  virtual bool
  PartialMerge(const rocksdb::Slice& key,
	       const rocksdb::Slice& left_operand,
	       const rocksdb::Slice& right_operand,
	       std::string* new_value,
	       rocksdb::Logger* logger) const override
  { engine e_ctxt;
    PlTermv av(6);
    static const PlAtom ATOM_partial("partial");

    if ( av[0].unify_term(merger.term()) &&
	 av[1].unify_atom(ATOM_partial) &&
	 unify(av[2], key, type.key) &&
	 unify(av[3], left_operand, type.value) &&
	 unify(av[4], right_operand, type.value) )
      return call_merger(type, av, new_value, logger);
    else
      return log_exception(logger);
  }

  virtual const char*
  Name() const override
  { return "PrologMergeOperator";
  }
};

template<typename Number_t> [[nodiscard]]
static int
cmp_number(const void *v1, const void *v2)
{ const auto i1 = static_cast<const Number_t *>(v1);
  const auto i2 = static_cast<const Number_t *>(v2);

  return *i1 > *i2 ? 1 : *i1 < *i2 ? -1 : 0;
}

template<typename Number_t>
static void
sort_numbers(std::string *str)
{ const auto s = str->data();
  const auto len = str->length();
  if ( len == 0 )
    return;
  auto ip = reinterpret_cast<Number_t *>(s);
  auto op = ip+1;
  const auto ep = reinterpret_cast<Number_t *>(s+len);
  Number_t cv;

  qsort(s, len / sizeof ip, sizeof ip, cmp_number<Number_t>);
  cv = *ip;
  for ( ip++; ip < ep; ip++ )
  { if ( *ip != cv )
      *op++ = cv = *ip;
  }
  str->resize(static_cast<size_t>(reinterpret_cast<char *>(op) - s));
}


void
sort(std::string *str, blob_type type)
{ switch ( type )
  { case BLOB_INT32:
      sort_numbers<int32_t>(str);
      break;
    case BLOB_INT64:
      sort_numbers<int64_t>(str);
      break;
    case BLOB_FLOAT32:
      sort_numbers<float>(str);
      break;
    case BLOB_FLOAT64:
      sort_numbers<double>(str);
      break;
    default:
      assert(0);
      break;
  }
}


class ListMergeOperator : public rocksdb::MergeOperator
{
private:
  dbref_type_kv type;
  merger_t      builtin_merger;
  PlRecord      merger;

public:
  explicit ListMergeOperator(const dbref& ref)
    : type(ref.type), builtin_merger(ref.builtin_merger), merger(ref.merger.duplicate())
  { }

  ~ListMergeOperator()
  { merger.erase();
  }

  virtual bool
  FullMerge(const rocksdb::Slice& key,
	    const rocksdb::Slice* existing_value,
	    const std::deque<std::string>& operand_list,
	    std::string* new_value,
	    rocksdb::Logger* logger) const override
  { (void)key;
    (void)logger;
    std::string s(existing_value ? existing_value->ToString() : "");

    for (const auto& value : operand_list)
    { s += value;
    }

    if ( builtin_merger == MERGE_SET )
      sort(&s, type.value);
    *new_value = s;
    return true;
  }

  virtual bool
  PartialMerge(const rocksdb::Slice& key,
	       const rocksdb::Slice& left_operand,
	       const rocksdb::Slice& right_operand,
	       std::string* new_value,
	       rocksdb::Logger* logger) const override
  { (void)key;
    (void)logger;
    std::string s(left_operand.ToString());
    s += right_operand.ToString();

    if ( builtin_merger == MERGE_SET )
      sort(&s, type.value);
    *new_value = s;
    return true;
  }

  virtual const char*
  Name() const override
  { return "ListMergeOperator";
  }
};


		 /*******************************
		 *	    PREDICATES		*
		 *******************************/

static void
get_blob_type(PlTerm t, blob_type *key_type, merger_t *m)
{ PlAtom a(PlAtom::null);

  static const PlFunctor FUNCTOR_list1("list", 1);
  static const PlFunctor FUNCTOR_set1("set", 1);
  static const PlAtom ATOM_atom("atom");
  static const PlAtom ATOM_string("string");
  static const PlAtom ATOM_binary("binary");
  static const PlAtom ATOM_term("term");
  static const PlAtom ATOM_int32("int32");
  static const PlAtom ATOM_int64("int64");
  static const PlAtom ATOM_float("float");
  static const PlAtom ATOM_double("double");

  if ( m && t.is_functor(FUNCTOR_list1) )
  { *m = MERGE_LIST;
    t[1].get_atom_ex(&a);
  } else if ( m && t.is_functor(FUNCTOR_set1) )
  { *m = MERGE_SET;
    t[1].get_atom_ex(&a);
  } else
    t.get_atom_ex(&a);

  if ( !m || *m == MERGE_NONE )
  {      if ( ATOM_atom   == a ) { *key_type = BLOB_ATOM;   return; }
    else if ( ATOM_string == a ) { *key_type = BLOB_STRING; return; }
    else if ( ATOM_binary == a ) { *key_type = BLOB_BINARY; return; }
    else if ( ATOM_term   == a ) { *key_type = BLOB_TERM;   return; }
  }

       if ( ATOM_int32  == a ) { *key_type = BLOB_INT32;   return; }
  else if ( ATOM_int64  == a ) { *key_type = BLOB_INT64;   return; }
  else if ( ATOM_float  == a ) { *key_type = BLOB_FLOAT32; return; }
  else if ( ATOM_double == a ) { *key_type = BLOB_FLOAT64; return; }
  else throw PlDomainError("rocks_type", t);
}


static void
lookup_read_optdef_and_apply(rocksdb::ReadOptions *options,
			     PlAtom name, PlTerm opt)
{
  typedef void (*ReadOptdefAction)(rocksdb::ReadOptions *options, PlTerm t);

  struct ReadOptdef
  { const PlAtom name;
    const ReadOptdefAction action;

    ReadOptdef(const char *_name, ReadOptdefAction _action)
      : name(_name), action(_action) { }
    ReadOptdef(PlAtom _atom, ReadOptdefAction _action)
      : name(_atom), action(_action) { }
  };

  // TODO: #define RD_ODEF [options](PlTerm arg)
  #define RD_ODEF [](rocksdb::ReadOptions *o, PlTerm arg)

  static const ReadOptdef read_optdefs[] =
    {       // "snapshot" const Snapshot*
	    // "iterate_lower_bound" const rocksdb::Slice*
	    // "iterate_upper_bound" const rocksdb::Slice*
      ReadOptdef("readahead_size",                       RD_ODEF {
	       o->readahead_size                       = arg.as_size_t(); } ),
      ReadOptdef("max_skippable_internal_keys",          RD_ODEF {
	       o->max_skippable_internal_keys          = arg.as_uint64_t(); } ),
	      // "read_tier" ReadTier
      ReadOptdef("verify_checksums",                     RD_ODEF {
	       o->verify_checksums                     = arg.as_bool(); } ),
      ReadOptdef("fill_cache",                           RD_ODEF {
	       o->fill_cache                           = arg.as_bool(); } ),
      ReadOptdef("tailing",                              RD_ODEF {
	       o->tailing                              = arg.as_bool(); } ),
	      // "managed" - not used any more
      ReadOptdef("total_order_seek",                     RD_ODEF {
	       o->total_order_seek                     = arg.as_bool(); } ),
      ReadOptdef("auto_prefix_mode",                     RD_ODEF {
	       o->auto_prefix_mode                     = arg.as_bool(); } ),
      ReadOptdef("prefix_same_as_start",                 RD_ODEF {
	       o->prefix_same_as_start                 = arg.as_bool(); } ),
      ReadOptdef("pin_data",                             RD_ODEF {
	       o->pin_data                             = arg.as_bool(); } ),
      ReadOptdef("background_purge_on_iterator_cleanup", RD_ODEF {
	       o->background_purge_on_iterator_cleanup = arg.as_bool(); } ),
      ReadOptdef("ignore_range_deletions",               RD_ODEF {
	       o->ignore_range_deletions               = arg.as_bool(); } ),
	      // "table_filter" std::function<bool(const TableProperties&)>
	// TODO: "iter_start_seqnum" removed from rocksdb/include/options.h?
	// {         "iter_start_seqnum",                    RD_ODEF {
	//         o->iter_start_seqnum                    = static_cast<SequenceNumber>(arg); } },
	//       "timestamp" rocksdb::Slice*
	//       "iter_start_ts" rocksdb::Slice*
      ReadOptdef("deadline",                             RD_ODEF {
	       o->deadline                             = static_cast<std::chrono::microseconds>(arg.as_int64_t()); } ),
      ReadOptdef("io_timeout",                           RD_ODEF {
	       o->io_timeout                           = static_cast<std::chrono::microseconds>(arg.as_int64_t()); } ),
      ReadOptdef("value_size_soft_limit",                RD_ODEF {
	       o->value_size_soft_limit                = arg.as_uint64_t(); } ),

      ReadOptdef(PlAtom(PlAtom::null), nullptr)
    };

  #undef RD_ODEF

  for ( auto def=read_optdefs; def->name.not_null(); def++ )
  { if ( def->name == name )
    { def->action(options, opt[1]);
      return;
    }
  }
  throw PlTypeError("option", opt);
}


static void
lookup_write_optdef_and_apply(rocksdb::WriteOptions *options,
			      PlAtom name, PlTerm opt)
{
  typedef void (*WriteOptdefAction)(rocksdb::WriteOptions *o, PlTerm t);

  struct WriteOptdef
  { const PlAtom name;
    const WriteOptdefAction action;

    WriteOptdef(const char *_name, WriteOptdefAction _action)
      : name(_name), action(_action) { }
    WriteOptdef(PlAtom _atom, WriteOptdefAction _action)
      : name(_atom), action(_action) { }
  };

  // TODO: #define WR_ODEF [options](PlTerm arg)
  #define WR_ODEF [](rocksdb::WriteOptions *o, PlTerm arg)

  static const WriteOptdef optdefs[] =
    { WriteOptdef("sync",                           WR_ODEF {
		o->sync                           = arg.as_bool(); } ),
      WriteOptdef("disableWAL",                     WR_ODEF {
		o->disableWAL                     = arg.as_bool(); } ),
      WriteOptdef("ignore_missing_column_families", WR_ODEF {
		o->ignore_missing_column_families = arg.as_bool(); } ),
      WriteOptdef("no_slowdown",                    WR_ODEF {
		o->no_slowdown                    = arg.as_bool(); } ),
      WriteOptdef("low_pri",                        WR_ODEF {
		o->low_pri                        = arg.as_bool(); } ),
      WriteOptdef("memtable_insert_hint_per_batch", WR_ODEF {
		o->memtable_insert_hint_per_batch = arg.as_bool(); } ),
      //          "timestamp" rocksdb::Slice*

      WriteOptdef(PlAtom(PlAtom::null), nullptr)
    };

  #undef WR_ODEF

  for ( auto def=optdefs; def->name.not_null(); def++ )
  { if ( def->name == name )
    { def->action(options, opt[1]);
      return;
    }
  }
  throw PlTypeError("option", opt);
}

static void
options_set_InfoLogLevel(rocksdb::Options *options, PlTerm arg)
{ rocksdb::InfoLogLevel log_level;

  static const PlAtom ATOM_debug("debug");
  static const PlAtom ATOM_info("info");
  static const PlAtom ATOM_warn("warn");
  static const PlAtom ATOM_error("error");
  static const PlAtom ATOM_fatal("fatal");
  static const PlAtom ATOM_header("header");

  const auto arg_a = arg.as_atom();
       if ( arg_a == ATOM_debug  ) log_level = rocksdb::DEBUG_LEVEL;
  else if ( arg_a == ATOM_info   ) log_level = rocksdb::INFO_LEVEL;
  else if ( arg_a == ATOM_warn   ) log_level = rocksdb::WARN_LEVEL;
  else if ( arg_a == ATOM_error  ) log_level = rocksdb::ERROR_LEVEL;
  else if ( arg_a == ATOM_fatal  ) log_level = rocksdb::FATAL_LEVEL;
  else if ( arg_a == ATOM_header ) log_level = rocksdb::HEADER_LEVEL;
  else throw PlTypeError("InfoLogLevel", arg); // TODO: this causes SIGSEGV
  options->info_log_level = log_level;
}


static void
lookup_open_optdef_and_apply(rocksdb::Options *options,
			     PlAtom name, PlTerm opt)
{
  typedef void (*OpenOptdefAction)(rocksdb::Options *options, PlTerm t);

  struct OpenOptdef
  { const PlAtom name;
    const OpenOptdefAction action;

    OpenOptdef(const char *_name, OpenOptdefAction _action)
      : name(_name), action(_action) { }
    OpenOptdef(PlAtom _atom, OpenOptdefAction _action)
      : name(_atom), action(_action) { }
  };

  // TODO: #define ODEF [options](PlTerm arg)
  #define ODEF [](rocksdb::Options *o, PlTerm arg)

  static const OpenOptdef open_optdefs[] =
    { OpenOptdef("prepare_for_bulk_load",                   ODEF { if ( arg.as_bool() ) o->PrepareForBulkLoad(); } ), // TODO: what to do with false?
      OpenOptdef("optimize_for_small_db",                   ODEF { if ( arg.as_bool() ) o->OptimizeForSmallDb(); } ), // TODO: what to do with false? - there's no DontOptimizeForSmallDb()
#ifndef ROCKSDB_LITE
      OpenOptdef("increase_parallelism",                    ODEF { if ( arg.as_bool() ) o->IncreaseParallelism(); } ),
#endif
      OpenOptdef("create_if_missing",                       ODEF {
	       o->create_if_missing                       = arg.as_bool(); } ),
      OpenOptdef("create_missing_column_families",          ODEF {
	       o->create_missing_column_families          = arg.as_bool(); } ),
      OpenOptdef("error_if_exists",                         ODEF {
	       o->error_if_exists                         = arg.as_bool(); } ),
      OpenOptdef("paranoid_checks",                         ODEF {
	       o->paranoid_checks                         = arg.as_bool(); } ),
      OpenOptdef("track_and_verify_wals_in_manifest",       ODEF {
	       o->track_and_verify_wals_in_manifest       = arg.as_bool(); } ),
      //         "env" Env::Default
      //         "rate_limiter" - shared_ptr<RateLimiter>
      //         "sst_file_manager" - shared_ptr<SstFileManager>
      //         "info_log" - shared_ptr<rocksdb::Logger> - see comment in ../README.md
      OpenOptdef("info_log_level",                          options_set_InfoLogLevel ),
      OpenOptdef("max_open_files",                          ODEF {
	       o->max_open_files                          = arg.as_int(); } ),
      OpenOptdef("max_file_opening_threads",                ODEF {
	       o-> max_file_opening_threads               = arg.as_int(); } ),
      OpenOptdef("max_total_wal_size",                      ODEF {
	       o->max_total_wal_size                      = arg.as_uint64_t(); } ),
      OpenOptdef("statistics",                              ODEF {
	       o->statistics = arg.as_bool() ? rocksdb::CreateDBStatistics() : nullptr; } ),
      OpenOptdef("use_fsync",                               ODEF {
	       o->use_fsync                               = arg.as_bool(); } ),
      //         "db_paths" - vector<DbPath>
      OpenOptdef("db_log_dir",                              ODEF {
	       o->db_log_dir                              = arg.as_string(PlEncoding::Locale); } ),
      OpenOptdef("wal_dir",                                 ODEF {
	       o->wal_dir                                 = arg.as_string(PlEncoding::Locale); } ),
      OpenOptdef("delete_obsolete_files_period_micros",     ODEF {
	       o->delete_obsolete_files_period_micros     = arg.as_uint64_t(); } ),
      OpenOptdef("max_background_jobs",                     ODEF {
	       o->max_background_jobs                     = arg.as_int(); } ),
      //         "base_background_compactions" is obsolete
      //         "max_background_compactions" is obsolete
      OpenOptdef("max_subcompactions",                      ODEF {
	       o->max_subcompactions                      = arg.as_uint32_t(); } ),
      //         "max_background_flushes" is obsolete
      OpenOptdef("max_log_file_size",                       ODEF {
	       o->max_log_file_size                       = arg.as_size_t(); } ),
      OpenOptdef("log_file_time_to_roll",                   ODEF {
	       o->log_file_time_to_roll                   = arg.as_size_t(); } ),
      OpenOptdef("keep_log_file_num",                       ODEF {
	       o->keep_log_file_num                       = arg.as_size_t(); } ),
      OpenOptdef("recycle_log_file_num",                    ODEF {
	       o->recycle_log_file_num                    = arg.as_size_t(); } ),
      OpenOptdef("max_manifest_file_size",                  ODEF {
	       o->max_manifest_file_size                  = arg.as_uint64_t(); } ),
      OpenOptdef("table_cache_numshardbits",                ODEF {
	       o->table_cache_numshardbits                = arg.as_int(); } ),
      OpenOptdef("wal_ttl_seconds",                         ODEF {
	       o->WAL_ttl_seconds                         = arg.as_uint64_t(); } ),
      OpenOptdef("wal_size_limit_mb",                       ODEF {
	       o->WAL_size_limit_MB                       = arg.as_uint64_t(); } ),
      OpenOptdef("manifest_preallocation_size",             ODEF {
	       o->manifest_preallocation_size             = arg.as_size_t(); } ),
      OpenOptdef("allow_mmap_reads",                        ODEF {
	       o->allow_mmap_reads                        = arg.as_bool(); } ),
      OpenOptdef("allow_mmap_writes",                       ODEF {
	       o->allow_mmap_writes                       = arg.as_bool(); } ),
      OpenOptdef("use_direct_reads",                        ODEF {
	       o->use_direct_reads                        = arg.as_bool(); } ),
      OpenOptdef("use_direct_io_for_flush_and_compaction",  ODEF {
	       o->use_direct_io_for_flush_and_compaction  = arg.as_bool(); } ),
      OpenOptdef("allow_fallocate",                         ODEF {
	       o->allow_fallocate                         = arg.as_bool(); } ),
      OpenOptdef("is_fd_close_on_exec",                     ODEF {
	       o->is_fd_close_on_exec                     = arg.as_bool(); } ),
      //         "skip_log_error_on_recovery" is obsolete
      OpenOptdef("stats_dump_period_sec",                   ODEF {
	       o->stats_dump_period_sec                   = arg.as_uint32_t(); } ), // TODO: match: unsigned int stats_dump_period_sec
      OpenOptdef("stats_persist_period_sec",                ODEF {
	       o->stats_persist_period_sec                = arg.as_uint32_t(); } ), // TODO: match: unsigned int stats_persist_period_sec
      OpenOptdef("persist_stats_to_disk",                   ODEF {
	       o->persist_stats_to_disk                   = arg.as_bool(); } ),
      OpenOptdef("stats_history_buffer_size",               ODEF {
	       o->stats_history_buffer_size               = arg.as_size_t(); } ),
      OpenOptdef("advise_random_on_open",                   ODEF {
	       o->advise_random_on_open                   = arg.as_bool(); } ),
      OpenOptdef("db_write_buffer_size",                    ODEF {
	       o->db_write_buffer_size                    = arg.as_size_t(); } ),
      //         "write_buffer_manager" - shared_ptr<WriteBufferManager>
      //         "access_hint_on_compaction_start" - enum AccessHint
      //   TODO: "new_table_reader_for_compaction_inputs"  removed from rocksdb/include/options.h?
      // OpenOptdef("new_table_reader_for_compaction_inputs",  ODEF {
      //          o->new_table_reader_for_compaction_inputs  = arg.as_bool(); } ),
      OpenOptdef("compaction_readahead_size",               ODEF {
	       o->compaction_readahead_size               = arg.as_size_t(); } ),
      OpenOptdef("random_access_max_buffer_size",           ODEF {
	       o->random_access_max_buffer_size           = arg.as_size_t(); } ),
      OpenOptdef("writable_file_max_buffer_size",           ODEF {
	       o->writable_file_max_buffer_size           = arg.as_size_t(); } ),
      OpenOptdef("use_adaptive_mutex",                      ODEF {
	       o->use_adaptive_mutex                      = arg.as_bool(); } ),
      OpenOptdef("bytes_per_sync",                          ODEF {
	       o->bytes_per_sync                          = arg.as_uint64_t(); } ),
      OpenOptdef("wal_bytes_per_sync",                      ODEF {
	       o->wal_bytes_per_sync                      = arg.as_uint64_t(); } ),
      OpenOptdef("strict_bytes_per_sync",                   ODEF {
	       o->strict_bytes_per_sync                   = arg.as_bool(); } ),
      //         "listeners" - vector<shared_ptr<EventListener>>
      OpenOptdef("enable_thread_tracking",                  ODEF {
	       o->enable_thread_tracking                  = arg.as_bool(); } ),
      OpenOptdef("delayed_write_rate",                      ODEF {
	       o->delayed_write_rate                      = arg.as_uint64_t(); } ),
      OpenOptdef("enable_pipelined_write",                  ODEF {
	       o->enable_pipelined_write                  = arg.as_bool(); } ),
      OpenOptdef("unordered_write",                         ODEF {
	       o->unordered_write                         = arg.as_bool(); } ),
      OpenOptdef("allow_concurrent_memtable_write",         ODEF {
	       o->allow_concurrent_memtable_write         = arg.as_bool(); } ),
      OpenOptdef("enable_write_thread_adaptive_yield",      ODEF {
	       o->enable_write_thread_adaptive_yield      = arg.as_bool(); } ),
      OpenOptdef("max_write_batch_group_size_bytes",        ODEF {
	       o->max_write_batch_group_size_bytes        = arg.as_uint64_t(); } ),
      OpenOptdef("write_thread_max_yield_usec",             ODEF {
	       o->write_thread_max_yield_usec             = arg.as_uint64_t(); } ),
      OpenOptdef("write_thread_slow_yield_usec",            ODEF {
	       o->write_thread_slow_yield_usec            = arg.as_uint64_t(); } ),
      OpenOptdef("skip_stats_update_on_db_open",            ODEF {
	       o->skip_stats_update_on_db_open            = arg.as_bool(); } ),
      OpenOptdef("skip_checking_sst_file_sizes_on_db_open", ODEF {
	       o->skip_checking_sst_file_sizes_on_db_open = arg.as_bool(); } ),
      //         "wal_recovery_mode" - enum WALRecoveryMode
      OpenOptdef("allow_2pc",                               ODEF {
	       o->allow_2pc                               = arg.as_bool(); } ),
      //         "row_cache" - shared_ptr<Cache>
      //         "wal_filter" - WalFilter*
      OpenOptdef("fail_ifoptions_file_error",               ODEF {
	       o->fail_if_options_file_error              = arg.as_bool(); } ),
      OpenOptdef("dump_malloc_stats",                       ODEF {
	       o->dump_malloc_stats                       = arg.as_bool(); } ),
      OpenOptdef("avoid_flush_during_recovery",             ODEF {
	       o->avoid_flush_during_recovery             = arg.as_bool(); } ),
      OpenOptdef("avoid_flush_during_shutdown",             ODEF {
	       o->avoid_flush_during_shutdown             = arg.as_bool(); } ),
      OpenOptdef("allow_ingest_behind",                     ODEF {
	       o->allow_ingest_behind                     = arg.as_bool(); } ),
      // TODO: "preserve_deletes" removed from rocksdb/include/options.h?
      // OpenOptdef("preserve_deletes",                        ODEF {
      //          o->preserve_deletes                        = arg.as_bool(); } ),
      OpenOptdef("two_write_queues",                        ODEF {
	       o->two_write_queues                        = arg.as_bool(); } ),
      OpenOptdef("manual_wal_flush",                        ODEF {
	       o->manual_wal_flush                        = arg.as_bool(); } ),
      OpenOptdef("atomic_flush",                            ODEF {
	       o->atomic_flush                            = arg.as_bool(); } ),
      OpenOptdef("avoid_unnecessary_blocking_io",           ODEF {
	       o->avoid_unnecessary_blocking_io           = arg.as_bool(); } ),
      OpenOptdef("write_dbid_to_manifest",                  ODEF {
	       o->write_dbid_to_manifest                  = arg.as_bool(); } ),
      OpenOptdef("log_readahead_size",                      ODEF {
	       o->write_dbid_to_manifest                  = arg.as_bool(); } ),
      //         "file_checksum_gen_factory" - std::shared_ptr<FileChecksumGenFactory>
      OpenOptdef("best_efforts_recovery",                           ODEF {
	       o->best_efforts_recovery                   = arg.as_bool(); } ),
      OpenOptdef("max_bgerror_resume_count",                ODEF {
	       o->max_bgerror_resume_count                = arg.as_int(); } ),
      OpenOptdef("bgerror_resume_retry_interval",           ODEF {
	       o->bgerror_resume_retry_interval           = arg.as_uint64_t(); } ),
      OpenOptdef("allow_data_in_errors",                    ODEF {
	       o->allow_data_in_errors                    = arg.as_bool(); } ),
      OpenOptdef("db_host_id",                              ODEF {
	       o->db_host_id                            = arg.as_string(PlEncoding::Locale); } ),
      //         "checksum_handoff_file_types" - FileTypeSet

      { PlAtom(PlAtom::null), nullptr }
};

#undef ODEF

  for ( auto def=open_optdefs; def->name.not_null(); def++ )
  { if ( def->name == name )
    { def->action(options, opt[1]);
      return;
    }
  }
  throw PlTypeError("option", opt);
}


PREDICATE(rocks_open_, 3)
{ rocksdb::Options options;
  options.create_if_missing = true; // caller can override this default value
  char *fn; // from A1 - assumes that it's already absolute file name
  blob_type key_type   = BLOB_ATOM;
  blob_type value_type = BLOB_ATOM;
  merger_t builtin_merger = MERGE_NONE;
  PlAtom alias(PlAtom::null);
  PlRecord merger(PlRecord::null);
  bool read_only = false;

  static const PlAtom ATOM_key("key");
  static const PlAtom ATOM_value("value");
  static const PlAtom ATOM_alias("alias");
  static const PlAtom ATOM_merge("merge");
  static const PlAtom ATOM_open("open");
  static const PlAtom ATOM_mode("mode");
  static const PlAtom ATOM_read_write("read_write");
  static const PlAtom ATOM_read_only("read_only");

  PlCheckFail(Plx_get_file_name(A1.C_, &fn, PL_FILE_OSPATH));
  PlTerm_tail tail(A3);
  PlTerm_var opt;
  while ( tail.next(opt) )
  { PlAtom name(PlAtom::null);
    size_t arity;

    PlCheckFail(opt.name_arity(&name, &arity));
    if ( arity == 1 )
    { if ( ATOM_key == name )
	get_blob_type(opt[1], &key_type, static_cast<merger_t *>(nullptr));
      else if ( ATOM_value == name )
	get_blob_type(opt[1], &value_type, &builtin_merger);
      else if ( ATOM_merge == name )
	merger = opt[1].record();
      else if ( ATOM_alias == name )
      { alias = opt[1].as_atom();
      } else if ( ATOM_mode == name )
      { const auto a = opt[1].as_atom();
	if ( ATOM_read_write == a )
	  read_only = false;
	else if ( ATOM_read_only == a )
	  read_only = true;
	else
	  throw PlDomainError("mode_option", opt[1]);
      } else
      { lookup_open_optdef_and_apply(&options, name, opt);
      }
    } else
      throw PlTypeError("option", opt);
  }

  if ( alias.not_null() )
  { const auto existing = rocks_get_alias(alias);
    if ( existing.not_null() )
      throw PlPermissionError("alias", "rocksdb", PlTerm_atom(alias));
  }

  // Allocating the blob uses unique_ptr<dbref> so that it'll be
  // deleted if an error happens - the auto-deletion is disabled by
  // ref.release() before returning success.

  auto ref = std::make_unique<dbref>();
  ref->merger         = merger;
  ref->builtin_merger = builtin_merger;
  ref->type.key       = key_type;
  ref->type.value     = value_type;
  ref->pathname       = PlAtom(fn);
  ref->pathname.register_ref();
  ref->name           = alias;
  if ( ref->name.not_null() )
    ref->name.register_ref();

  if ( ref->merger.not_null() )
    options.merge_operator.reset(new PrologMergeOperator(*ref));
  else if ( builtin_merger != MERGE_NONE )
    options.merge_operator.reset(new ListMergeOperator(*ref));
  ok_or_throw_fail(read_only
		   ? rocksdb::DB::OpenForReadOnly(options, fn, &ref->db)
		   : rocksdb::DB::Open(options, fn, &ref->db),
		   ref.get());

  if ( ref->name.is_null() )
  { PlCheckFail(A2.unify_blob(ref.get()));
  } else
  { PlTerm_var tmp;
    PlCheckFail(tmp.unify_blob(ref.get()));
    rocks_add_alias(ref->name, tmp.as_atom());
    PlCheckFail(A2.unify_atom(ref->name));
  }

  (void)ref.release(); // ref now owned by Prolog, deleted in release_rocks_ref_()
  return true;
}


PREDICATE(rocks_close, 1)
{ const auto ref = get_rocks(A1, false);
  if ( !ref )
    return true;

  // The following code is a subset of dbref::~dbref(), and also can
  // throw a Prolog error if ref->db->Close() fails.

  if ( ref->name.not_null() )
  { rocks_unalias(ref->name);
    // ref->name is needed by write() callabck, so don't do this: ref->name.unregister_ref()
  }

  { auto db = ref->db;
    ref->db = nullptr;
    if ( db )
      ok_or_throw_fail(db->Close(), ref);
  }

  return true;
}


PREDICATE(rocks_alias_lookup, 2)
{ PlAtom a(rocks_get_alias(A1.as_atom()));
  if ( a.not_null() )
    return A2.unify_atom(a);
  return false;
}


[[nodiscard]]
static rocksdb::WriteOptions
write_options(PlTerm options_term)
{ rocksdb::WriteOptions options;
  PlTerm_tail tail(options_term);
  PlTerm_var opt;
  while ( tail.next(opt) )
  { PlAtom name(PlAtom::null);
    size_t arity;

    PlCheckFail(opt.name_arity(&name, &arity));
    if ( arity == 1 )
      lookup_write_optdef_and_apply(&options, name, opt);
    else
      throw PlTypeError("option", opt);
  }
  return options;
}


PREDICATE(rocks_put, 4)
{ const auto ref = get_rocks(A1);
  const auto key = get_slice(A2, ref->type.key);

  if ( ref->builtin_merger == MERGE_NONE )
  { const auto value = get_slice(A3, ref->type.value);
    ok_or_throw_fail(ref->db->Put(write_options(A4), key->slice(), value->slice()), ref);
  } else
  { PlTerm_tail list(A3);
    PlTerm_var tmp;
    std::string value;

    while ( list.next(tmp) )
    { const auto s = get_slice(tmp, ref->type.value);
      value += s->ToString();
    }

    if ( ref->builtin_merger == MERGE_SET )
      sort(&value, ref->type.value);

    ok_or_throw_fail(ref->db->Put(write_options(A4), key->slice(), value), ref);
  }

  return true;
}

PREDICATE(rocks_merge, 4)
{ const auto ref = get_rocks(A1);
  if ( ref->merger.is_null() && ref->builtin_merger == MERGE_NONE )
    throw PlPermissionError("merge", "rocksdb", A1);

  const auto key = get_slice(A2,ref->type.key);
  const auto value = get_slice(A3, ref->type.value);

  ok_or_throw_fail(ref->db->Merge(write_options(A4), key->slice(), value->slice()), ref);

  return true;
}

[[nodiscard]]
static rocksdb::ReadOptions
read_options(PlTerm options_term)
{ rocksdb::ReadOptions options;
  PlTerm_tail tail(options_term);
  PlTerm_var opt;
  while ( tail.next(opt) )
  { PlAtom name(PlAtom::null);
    size_t arity;
    PlCheckFail(opt.name_arity(&name, &arity));
    if ( arity == 1 )
      lookup_read_optdef_and_apply(&options, name, opt);
    else
      throw PlTypeError("option", opt);
  }
  return options;
}

PREDICATE(rocks_get, 4)
{ const auto ref = get_rocks(A1);
  std::string value;
  const auto key = get_slice(A2, ref->type.key);

  ok_or_throw_fail(ref->db->Get(read_options(A4), key->slice(), &value), ref);
  return unify_value(A3, value, ref->builtin_merger, ref->type.value);
}

PREDICATE(rocks_delete, 3)
{ const auto ref = get_rocks(A1);
  const auto key = get_slice(A2, ref->type.key);

  ok_or_throw_fail(ref->db->Delete(write_options(A3), key->slice()), ref);
  return true;
}

enum enum_type
{ ENUM_NOT_INITIALIZED = 0,
  ENUM_ALL,  // TODO: not used?
  ENUM_FROM, // TODO: not used?
  ENUM_PREFIX
};

struct enum_state
{
  enum_state(dbref *_ref)
    : ref(_ref), type(ENUM_NOT_INITIALIZED)
  { }
  std::unique_ptr<rocksdb::Iterator> it;
  dbref    *ref;
  enum_type type;
  std::string prefix;
};

[[nodiscard]]
static bool
unify_enum_key(PlTerm t, const enum_state& state)
{ if ( state.type == ENUM_PREFIX )
  { rocksdb::Slice k(state.it->key());

    if ( k.size_ >= state.prefix.length() &&
	 memcmp(k.data_, state.prefix.data(), state.prefix.length()) == 0 )
    { k.data_ += state.prefix.length();
      k.size_ -= state.prefix.length();

      return unify(t, k, state.ref->type.key);
    } else
      return false;
  } else
  { return unify(t, state.it->key(), state.ref->type.key);
  }
}


[[nodiscard]]
static bool
enum_key_prefix(const enum_state& state)
{ if ( state.type == ENUM_PREFIX )
  { rocksdb::Slice k(state.it->key());
    return ( k.size_ >= state.prefix.length() &&
	     memcmp(k.data_, state.prefix.data(), state.prefix.length()) == 0 );
  } else
    return true;
}


[[nodiscard]]
static foreign_t
rocks_enum(PlTermv PL_av, int ac, enum_type type, PlControl handle, rocksdb::ReadOptions options)
{ PlForeignContextPtr<enum_state> state(handle);

  switch ( handle.foreign_control() )
  { case PL_FIRST_CALL:
      state.set(new enum_state(get_rocks(A1)));
      if ( ac >= 4 )
      { if ( !(state->ref->type.key == BLOB_ATOM ||
	       state->ref->type.key == BLOB_STRING ||
	       state->ref->type.key == BLOB_BINARY) )
	  throw PlPermissionError("enum", "rocksdb", A1);

	const std::string prefix = A4.get_nchars(REP_UTF8|CVT_IN|CVT_EXCEPTION);

	if ( type == ENUM_PREFIX )
	{ state->prefix = prefix;
	}
	state->it.reset(state->ref->db->NewIterator(options));
	state->it->Seek(prefix);
      } else
      { state->it.reset(state->ref->db->NewIterator(options));
	state->it->SeekToFirst();
      }
      state->type = type;
      [[fallthrough]];
    case PL_REDO:
    { PlFrame fr;
      for ( ; state->it->Valid(); state->it->Next() )
      { if ( unify_enum_key(A2, *state) &&
	     unify_value(A3, state->it->value(),
			 state->ref->builtin_merger, state->ref->type.value) )
	{ state->it->Next();
	  if ( state->it->Valid() && enum_key_prefix(*state) )
	  { PL_retry_address(state.keep());
	  } else
	  { return true;
	  }
	}
	fr.rewind();
      }
      return false;
    }
    case PL_PRUNED:
      return true;
    default:
      assert(0);
      return false;
  }
  return false;
}


PREDICATE_NONDET(rocks_enum, 4)
{ return rocks_enum(PL_av, 3, ENUM_ALL, handle, read_options(A4));
}


PREDICATE_NONDET(rocks_enum_from, 5)
{ return rocks_enum(PL_av, 4, ENUM_FROM, handle, read_options(A5));
}


PREDICATE_NONDET(rocks_enum_prefix, 5)
{ return rocks_enum(PL_av, 4, ENUM_PREFIX, handle, read_options(A5));
}


static void
batch_operation(const dbref& ref, rocksdb::WriteBatch *batch, PlTerm e)
{ PlAtom name(PlAtom::null);
  size_t arity;

  static PlAtom ATOM_delete("delete");
  static PlAtom ATOM_put("put");

  PlCheckFail(e.name_arity(&name, &arity));
  if ( ATOM_delete == name && arity == 1 )
  { const auto key = get_slice(e[1], ref.type.key);
    batch->Delete(key->slice());
  } else if ( ATOM_put == name && arity == 2 )
  { const auto key = get_slice(e[1], ref.type.key);
    const auto value = get_slice(e[2], ref.type.value);
    batch->Put(key->slice(), value->slice());
  } else
  { throw PlDomainError("rocks_batch_operation", e);
  }
}


PREDICATE(rocks_batch, 3)
{ const auto ref = get_rocks(A1);
  rocksdb::WriteBatch batch;
  PlTerm_tail tail(A2);
  PlTerm_var e;

  while ( tail.next(e) )
  { batch_operation(*ref, &batch, e);
  }

  ok_or_throw_fail(ref->db->Write(write_options(A3), &batch), ref);
  return true;
}


PREDICATE(rocks_property, 3)
{ const auto ref = get_rocks(A1);
  static PlAtom ATOM_estimate_num_keys("estimate_num_keys");

  const auto prop = A2.as_atom();

  if ( ATOM_estimate_num_keys == prop )
  { uint64_t value;

    return ref->db->GetIntProperty("rocksdb.estimate-num-keys", &value) &&
	     A3.unify_integer(value);
  } else
     throw PlDomainError("rocks_property", A2);
}
