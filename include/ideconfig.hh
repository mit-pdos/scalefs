#pragma once

#if defined(HW_qemu)
#define MEMIDE        1
#define AHCIIDE       0

#elif defined(HW_ben)
#define MEMIDE        1
#define AHCIIDE       0
#endif

#ifndef MEMIDE
#define MEMIDE 1
#endif
#ifndef AHCIIDE
#define AHCIIDE 0
#endif
