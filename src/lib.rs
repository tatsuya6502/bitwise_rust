//
// bitwise_nif : examples of NIF scheduling
//
// This is a Rust port of Steve Vinoski's bitwise, NIF examples
// showing Erlang scheduler concerns. The original C program
// can be found at: https://github.com/vinoski/bitwise
//

#[macro_use]
extern crate ruster_unsafe;
use ruster_unsafe::*;

use std::mem::uninitialized;
use std::slice;

extern crate libc;
use libc::c_uint;

/// (module doc)
/// The exor functions here take a binary and a byte and generate a new
/// binary by applying xor of the byte value to each byte of the binary.
/// It returns a tuple of the new binary and a count of how many times
/// the Erlang scheduler thread is yielded during processing of the binary.

/// Create NIF module data and init function.
/// Note that exor, exor_bad, and exor_dirty all run the same Rust function,
/// but exor and exor_bad run it on a regular scheduler thread whereas
/// exor_dirty runs it on a dirty CPU scheduler thread
nif_init!(b"bitwise\0", Some(load), Some(reload), Some(upgrade), Some(unload),
          nif!(b"exor\0",       2, exor),
          nif!(b"exor_bad\0",   2, exor),
          // nif!(b"exor_yield\0", 2, exor_yield),
          nif!(b"exor_dirty\0", 2, exor, ERL_NIF_DIRTY_JOB_CPU_BOUND)
          );

/// Does nothing, reports success
extern "C" fn load(_env: *mut ErlNifEnv,
                   _priv_data: *mut *mut c_void,
                   _load_info: ERL_NIF_TERM)-> c_int { 0 }

/// Does nothing, reports success
extern "C" fn reload(_env: *mut ErlNifEnv,
                     _priv_data: *mut *mut c_void,
                     _load_info: ERL_NIF_TERM) -> c_int { 0 }

/// Does nothing, reports success
extern "C" fn upgrade(_env: *mut ErlNifEnv,
                      _priv_data: *mut *mut c_void,
                      _old_priv_data: *mut *mut c_void,
                      _load_info: ERL_NIF_TERM) -> c_int { 0 }

/// Does nothing, reports success
extern "C" fn unload(_env: *mut ErlNifEnv,
                     _priv_data: *mut c_void) {}

/// Erlang: -spec exor(binary(), byte::0..255) -> binary().
///
/// exor misbehaves on a regular scheduler thread when the incomng binary is
/// large because it blocks the thread for too long. But it works fine on a
/// dirty scheduler.
extern "C" fn exor(env: *mut ErlNifEnv,
                   argc: c_int,
                   args: *const ERL_NIF_TERM) -> ERL_NIF_TERM {
    unsafe {
        let mut bin: ErlNifBinary = uninitialized();
        let mut outbin: ErlNifBinary = uninitialized();
        let mut val: c_uint = uninitialized();

        if argc != 2
            || 0 == enif_inspect_binary(env, *args, &mut bin)
            || 0 == enif_get_uint(env, *args.offset(1), &mut val)
            || val > 255 {
            return enif_make_badarg(env);
        }
        if bin.size == 0 {
            return *args;
        }

        enif_alloc_binary(bin.size, &mut outbin);

        let bin_slice: &[u8] = slice::from_raw_parts(bin.data, bin.size);
        let outbin_slice: &mut[u8] = slice::from_raw_parts_mut(outbin.data, bin.size);

        do_xor(bin_slice, outbin_slice, val as u8);

        // @TODO: Implement enif_make_tuple2() and friends in ruster_unsafe
        // enif_make_tuple2(env,
        //                  enif_make_binary(env, &mut outbin),
        //                  enif_make_int(env, 0))
        enif_make_binary(env, &mut outbin)
    }
}

/// Reads source bytes, applies xor xor_byte to each byte, and
/// stores them to target.
fn do_xor(source: &[u8], target: &mut[u8], xor_byte: u8) {
    for (src_b, tgt_b) in source.iter().zip(target.iter_mut()) {
        (*tgt_b) = (*src_b) ^ xor_byte;
    }
}

// C lang
//
// / exor_yield just schedules exor2 for execution, providing an initial
// / guess of 4MB for the max number of bytes to process before yielding the
// / scheduler thread
// static ERL_NIF_TERM
// exor_yield(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
// {
//     ErlNifResourceType* res_type = (ErlNifResourceType*)enif_priv_data(env);
//     ERL_NIF_TERM newargv[6];
//     ErlNifBinary bin;
//     unsigned val;
//     void* res;
//
//     if (argc != 2 || !enif_inspect_binary(env, argv[0], &bin) ||
//         !enif_get_uint(env, argv[1], &val) || val > 255)
//         return enif_make_badarg(env);
//     if (bin.size == 0)
//         return argv[0];
//     newargv[0] = argv[0];
//     newargv[1] = argv[1];
//     newargv[2] = enif_make_ulong(env, 4194304);
//     newargv[3] = enif_make_ulong(env, 0);
//     res = enif_alloc_resource(res_type, bin.size);
//     newargv[4] = enif_make_resource(env, res);
//     newargv[5] = enif_make_int(env, 0);
//     enif_release_resource(res);
//     return enif_schedule_nif(env, "exor2", 0, exor2, 6, newargv);
// }

// C lang
//
// / exor2 is an "internal NIF" scheduled by exor_yield above. It takes the
// / binary and byte arguments, same as the other functions here, but also
// / takes a count of the max number of bytes to process per timeslice, the
// / offset into the binary at which to start processing, the resource type
// / holding the resulting data, and the number of times rescheduling is done
// / via enif_schedule_nif.
// static ERL_NIF_TERM
// exor2(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
// {
//     ErlNifResourceType* res_type = (ErlNifResourceType*)enif_priv_data(env);
//     unsigned long offset, i, end, max_per_slice;
//     struct timeval start, stop, slice;
//     int pct, total = 0, yields;
//     ERL_NIF_TERM newargv[6];
//     ERL_NIF_TERM result;
//     unsigned char byte;
//     ErlNifBinary bin;
//     unsigned val;
//     void* res;
//
//     if (argc != 6 || !enif_inspect_binary(env, argv[0], &bin) ||
//         !enif_get_uint(env, argv[1], &val) || val > 255 ||
//         !enif_get_ulong(env, argv[2], &max_per_slice) ||
//         !enif_get_ulong(env, argv[3], &offset) ||
//         !enif_get_resource(env, argv[4], res_type, &res) ||
//         !enif_get_int(env, argv[5], &yields))
//         return enif_make_badarg(env);
//     byte = (unsigned char)val;
//     end = offset + max_per_slice;
//     if (end > bin.size) end = bin.size;
//     i = offset;
//     while (i < bin.size) {
//         gettimeofday(&start, NULL);
//         do {
//             ((char*)res)[i] = bin.data[i] ^ byte;
//         } while (++i < end);
//         if (i == bin.size) break;
//         gettimeofday(&stop, NULL);
//         /* determine how much of the timeslice was used */
//         timersub(&stop, &start, &slice);
//         pct = (int)((slice.tv_sec*1000000+slice.tv_usec)/10);
//         total += pct;
//         if (pct > 100) pct = 100;
//         else if (pct == 0) pct = 1;
//         if (enif_consume_timeslice(env, pct)) {
//             /* the timeslice has been used up, so adjust our max_per_slice byte count based on
//              * the processing we've done, then reschedule to run again */
//             max_per_slice = i - offset;
//             if (total > 100) {
//                 int m = (int)(total/100);
//                 if (m == 1)
//                     max_per_slice -= (unsigned long)(max_per_slice*(total-100)/100);
//                 else
//                     max_per_slice = (unsigned long)(max_per_slice/m);
//             }
//             newargv[0] = argv[0];
//             newargv[1] = argv[1];
//             newargv[2] = enif_make_ulong(env, max_per_slice);
//             newargv[3] = enif_make_ulong(env, i);
//             newargv[4] = argv[4];
//             newargv[5] = enif_make_int(env, yields+1);
//             return enif_schedule_nif(env, "exor2", 0, exor2, argc, newargv);
//         }
//         end += max_per_slice;
//         if (end > bin.size) end = bin.size;
//     }
//     result = enif_make_resource_binary(env, res, res, bin.size);
//     return enif_make_tuple2(env, result, enif_make_int(env, yields));
// }
