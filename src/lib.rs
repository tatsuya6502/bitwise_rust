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
use libc::c_uchar;
use libc::c_uint;
use libc::c_ulong;

// @TODO: This need to be a module doc.
// (see https://doc.rust-lang.org/book/documentation.html#documenting-modules)
//
// The exor functions here take a binary and a byte and generate a new
// binary by applying xor of the byte value to each byte of the binary.
// It returns a tuple of the new binary and a count of how many times
// the Erlang scheduler thread is yielded during processing of the binary.
//
// Create NIF module data and init function.
// Note that exor, exor_bad, and exor_dirty all run the same Rust function,
// but exor and exor_bad run it on a regular scheduler thread whereas
// exor_dirty runs it on a dirty CPU scheduler thread.

// Erlang: Set the NIF name to b"bitwise\0"
// Elixir: Set the NIF name to b"Elixir.BitwiseNif\0"
nif_init!(b"Elixir.BitwiseNif\0",
          Some(load),    // on-load
          None,          // on-reload
          Some(upgrade), // on-upgrade
          None,          // on-unload
          nif!(b"exor\0",       2, exor),
          nif!(b"exor_bad\0",   2, exor),
          nif!(b"exor_yield\0", 2, exor_yield),
          nif!(b"exor_dirty\0", 2, exor, ERL_NIF_DIRTY_JOB_CPU_BOUND)
          );

const DEFAULT_MAX_BYTES_PER_SLICE: c_ulong = 4 * 1024 * 1024;
const TIMESLICE_EXHAUSTED: c_int = 1;

extern "C" fn load(_env: *mut ErlNifEnv,
                   _priv_data: *mut *mut c_void,
                   _load_info: ERL_NIF_TERM)-> c_int {
    0
}

extern "C" fn upgrade(_env: *mut ErlNifEnv,
                      _priv_data: *mut *mut c_void,
                      _old_priv_data: *mut *mut c_void,
                      _load_info: ERL_NIF_TERM)-> c_int {
    0
}


/// Erlang: -spec exor(binary(), byte::0..255) -> binary().
///
/// exor misbehaves on a regular scheduler thread when the incomng binary is
/// large because it blocks the thread for too long. But it works fine on a
/// dirty scheduler.
extern "C" fn exor(env: *mut ErlNifEnv,
                   argc: c_int,
                   args: *const ERL_NIF_TERM) -> ERL_NIF_TERM {
    let mut in_bin:  ErlNifBinary = unsafe { uninitialized() };
    let mut out_bin: ErlNifBinary = unsafe { uninitialized() };
    let mut byte: c_uint          = unsafe { uninitialized() };

    if argc != 2
        || 0 == unsafe { enif_inspect_binary(env, *args, &mut in_bin) }
        || 0 == unsafe { enif_get_uint(env, *args.offset(1), &mut byte) }
        || byte > 255 {
        return unsafe { enif_make_badarg(env) };
    }
    if in_bin.size == 0 {
        return unsafe { *args };
    }

    unsafe { enif_alloc_binary(in_bin.size, &mut out_bin) };

    let in_bin_slice  = unsafe { slice::from_raw_parts(in_bin.data, in_bin.size) };
    let out_bin_slice = unsafe { slice::from_raw_parts_mut(out_bin.data, in_bin.size) };

    apply_xor(in_bin_slice, out_bin_slice, byte as u8);

    let yields = 0;
    unsafe {make_tuple2(env,
                        &enif_make_binary(env, &mut out_bin),
                        &enif_make_int(env, yields)) }
}

/// Erlang: -spec exor_yield(binary(), byte::0..255) -> binary().
///
/// exor_yield just schedules exor2 for execution, providing an
/// initial guess of 4MB for the max number of bytes to process before
/// yielding the scheduler thread.
extern "C" fn exor_yield(env: *mut ErlNifEnv,
                         argc: c_int,
                         args: *const ERL_NIF_TERM) -> ERL_NIF_TERM {
    let mut in_bin: ErlNifBinary = unsafe { uninitialized() };
    let mut byte: c_uint = unsafe { uninitialized() };

    if argc != 2
        || 0 == unsafe { enif_inspect_binary(env, *args, &mut in_bin) }
        || 0 == unsafe { enif_get_uint(env, *args.offset(1), &mut byte) }
        || byte > 255 {
        return unsafe { enif_make_badarg(env) };
    }

    if in_bin.size == 0 {
        return unsafe { *args };
    }

    let res_type: *mut ErlNifResourceType =
        unsafe { enif_priv_data(env) } as *mut ErlNifResourceType;
    let res: *mut c_void = unsafe { enif_alloc_resource(res_type, in_bin.size) };

    let new_args = unsafe {
        [*args.offset(0),
         *args.offset(1),
         enif_make_ulong(env, DEFAULT_MAX_BYTES_PER_SLICE),
         enif_make_ulong(env, 0),
         enif_make_resource(env, res),
         enif_make_int(env, 0)]
    };

    unsafe { enif_release_resource(res) };
    unsafe { enif_schedule_nif(env, b"exor2\0" as *const u8, 0,
                               Some(exor2), 6, new_args.as_ptr()) }
}


/// exor2 is an "internal NIF" scheduled by exor_yield above. It takes
/// the binary and byte arguments, same as the other functions here,
/// but also takes a count of the max number of bytes to process per
/// timeslice, the offset into the binary at which to start
/// processing, the resource type holding the resulting data, and the
/// number of times rescheduling is done via enif_schedule_nif.
extern "C" fn exor2(env: *mut ErlNifEnv,
                    argc: c_int,
                    args: *const ERL_NIF_TERM) -> ERL_NIF_TERM {
    let res_type: *mut ErlNifResourceType =
        unsafe { enif_priv_data(env) } as *mut ErlNifResourceType;

    let mut in_bin: ErlNifBinary          = unsafe { uninitialized() };
    let mut val: c_uint                   = unsafe { uninitialized() };
    let mut max_bytes_per_slice: c_ulong  = unsafe { uninitialized() };
    let mut start_offset: c_ulong         = unsafe { uninitialized() };
    let mut res: *mut c_void              = unsafe { uninitialized() };
    let mut yields: c_int                 = unsafe { uninitialized() };

    if argc != 6
        || 0 == unsafe { enif_inspect_binary(env, *args, &mut in_bin) }
        || 0 == unsafe { enif_get_uint(env, *args.offset(1), &mut val) }
        || val > 255
        || 0 == unsafe { enif_get_ulong(env, *args.offset(2), &mut max_bytes_per_slice) }
        || 0 == unsafe { enif_get_ulong(env, *args.offset(3), &mut start_offset) }
        || 0 == unsafe { enif_get_resource(env, *args.offset(4), res_type, &mut res) }
        || 0 == unsafe { enif_get_int(env, *args.offset(5), &mut yields) } {
        return unsafe { enif_make_badarg(env) };
    }

    let byte = val as c_uchar;

    let mut pos = start_offset as usize;
    let mut end_offset = (start_offset + max_bytes_per_slice) as usize;
    if end_offset > in_bin.size {
        end_offset = in_bin.size;
    }
    let mut consumed_timeslice: c_int = 0;

    loop {
        // gettimeofday(&start, NULL);

        let in_bin_slice = unsafe {
            slice::from_raw_parts(in_bin.data.offset(pos as isize),
                                  end_offset - pos + 1)
        };
        let res_bin_slice = unsafe {
            slice::from_raw_parts_mut(res.offset(pos as isize) as *mut u8,
                                      end_offset - pos + 1)
        };

        apply_xor(in_bin_slice, res_bin_slice, byte as u8);

        pos = end_offset;
        if pos == in_bin.size {
            // We are done. Break out from the loop and return the result.
            break;
        }

        // gettimeofday(&stop, NULL);
        // determine how much of the timeslice was used
        // timersub(&stop, &start, &slice);
        // let percent: c_int = (slice.tv_sec*1000000+slice.tv_usec)/10) as c_int;
        let percent: c_int = 100;  // @TODO: Replace with above.
        consumed_timeslice += percent;

        let resp = unsafe { enif_consume_timeslice(env, fix_range(percent)) };
        if resp == TIMESLICE_EXHAUSTED {
            // the timeslice has been used up, so adjust our max_bytes_per_slice byte
            // count based on the processing we've done, then reschedule to run
            // again.
            let max_bytes_per_slice =
                adjust_slice_size((pos as c_ulong) - start_offset,
                                  consumed_timeslice as c_ulong);
            let new_args = unsafe {
                [*args.offset(0),
                 *args.offset(1),
                 enif_make_ulong(env, max_bytes_per_slice),
                 enif_make_ulong(env, pos as c_ulong),
                 *args.offset(4),
                 enif_make_int(env, yields + 1)]
            };
            return unsafe { enif_schedule_nif(env, b"exor2\0" as *const u8, 0,
                                              Some(exor2), argc, new_args.as_ptr()) };
        }
        end_offset += max_bytes_per_slice as usize;
        if end_offset > in_bin.size {
            end_offset = in_bin.size;
        }
    }

    // We are done. Return the result.
    let out_bin = unsafe { enif_make_resource_binary(env, res, res, in_bin.size) };
    unsafe { make_tuple2(env, &out_bin, &enif_make_int(env, yields)) }
}

fn apply_xor(source: &[u8], target: &mut[u8], byte: u8) {
    for (src_b, tgt_b) in source.iter().zip(target.iter_mut()) {
        (*tgt_b) = (*src_b) ^ byte;
    }
}

fn fix_range(percent: c_int) -> c_int {
    // enif_consume_timeslice only accepts percent between 1 and 100.
    if percent <= 0 {
        1
    } else if percent > 100 {
        100
    } else {
        percent
    }
}

fn adjust_slice_size(bytes_processed: c_ulong,
                     consumed_timeslice: c_ulong) -> c_ulong {
    if consumed_timeslice <= 100 {
        bytes_processed
    } else {
        let m = consumed_timeslice / 100;
        if m == 1 {
            bytes_processed - (bytes_processed * (consumed_timeslice - 100) / 100)
        } else {
           bytes_processed / m
        }
    }
}

fn make_tuple2(env: *mut ErlNifEnv,
               e1: *const ERL_NIF_TERM, e2: *const ERL_NIF_TERM) -> ERL_NIF_TERM {
    let tuple_elements = unsafe { [*e1, *e2] };
    unsafe { enif_make_tuple_from_array(env, tuple_elements.as_ptr(), 2) }
}
