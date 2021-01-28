// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma once

#include <cstdint>
#include <cassert>
#include <string>

#ifdef GOOGLE_FRAMEWORK

#include <glog/logging.h>
#include <glog/raw_logging.h>
#define RAW_CHECK_ONLY

#else

#define DCHECK(...) ;
#define LOG_ASSERT(...) ;
#define CHECK_EQ(...) std::cout

#ifdef NDEBUG
#define RAW_CHECK(_cond,_msg)
#ifdef __GNUC__
#define RAW_CHECK_ONLY __attribute__((unused))
#else
#define RAW_CHECK_ONLY
#endif
#else
#define RAW_CHECK(_cond,_msg) assert(_cond)
#define RAW_CHECK_ONLY
#endif

#endif

namespace pmwcas {

#ifdef _DEBUG
#define verify(exp) assert(exp)
#else
#define verify(exp) ((void)0)
#endif

#define MARK_UNREFERENCED(P) ((void)P)

#define PREFETCH_KEY_DATA(key) _mm_prefetch(key.data(), _MM_HINT_T0)
#define PREFETCH_NEXT_PAGE(delta) _mm_prefetch((char*)(delta->next_page), _MM_HINT_T0)

// Returns true if \a x is a power of two.
#define IS_POWER_OF_TWO(x) (x && (x & (x - 1)) == 0)

// Prevents a type from being copied or moved, both by construction or by assignment.
#define DISALLOW_COPY_AND_MOVE(className) \
    className(const className&) = delete; \
    className& operator=(const className&) = delete; \
    className(className&&) = delete; \
    className& operator=(className&&) = delete

#ifndef ALWAYS_ASSERT
#define ALWAYS_ASSERT(expr) (expr) ? (void)0 : abort()
#endif
} // namespace pmwcas
