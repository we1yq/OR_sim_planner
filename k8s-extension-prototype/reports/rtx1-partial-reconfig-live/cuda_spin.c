#include <dlfcn.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>

typedef int CUdevice;
typedef void *CUcontext;
typedef void *CUmodule;
typedef void *CUfunction;
typedef int CUresult;

#define CHECK(call) do { \
    CUresult _rc = (call); \
    if (_rc != 0) { \
        fprintf(stderr, "%s failed: %d\n", #call, _rc); \
        return 1; \
    } \
} while (0)

static const char *ptx =
".version 7.0\n"
".target sm_80\n"
".address_size 64\n"
".visible .entry spin_kernel(.param .u64 cycles) {\n"
"  .reg .pred %p;\n"
"  .reg .u64 %start, %now, %cycles, %diff;\n"
"  ld.param.u64 %cycles, [cycles];\n"
"  mov.u64 %start, %clock64;\n"
"loop:\n"
"  mov.u64 %now, %clock64;\n"
"  sub.u64 %diff, %now, %start;\n"
"  setp.lt.u64 %p, %diff, %cycles;\n"
"  @%p bra loop;\n"
"  ret;\n"
"}\n";

int main(int argc, char **argv) {
    int seconds = argc > 1 ? atoi(argv[1]) : 20;
    if (seconds <= 0) seconds = 20;
    void *cuda = dlopen("libcuda.so.1", RTLD_NOW);
    if (!cuda) {
        fprintf(stderr, "dlopen libcuda.so.1 failed: %s\n", dlerror());
        return 1;
    }

    CUresult (*cuInit)(unsigned int) = dlsym(cuda, "cuInit");
    CUresult (*cuDeviceGet)(CUdevice *, int) = dlsym(cuda, "cuDeviceGet");
    CUresult (*cuCtxCreate)(CUcontext *, unsigned int, CUdevice) = dlsym(cuda, "cuCtxCreate_v2");
    CUresult (*cuModuleLoadData)(CUmodule *, const void *) = dlsym(cuda, "cuModuleLoadData");
    CUresult (*cuModuleGetFunction)(CUfunction *, CUmodule, const char *) = dlsym(cuda, "cuModuleGetFunction");
    CUresult (*cuLaunchKernel)(CUfunction, unsigned int, unsigned int, unsigned int, unsigned int, unsigned int, unsigned int, unsigned int, void *, void **, void **) = dlsym(cuda, "cuLaunchKernel");
    CUresult (*cuCtxSynchronize)(void) = dlsym(cuda, "cuCtxSynchronize");
    if (!cuInit || !cuDeviceGet || !cuCtxCreate || !cuModuleLoadData || !cuModuleGetFunction || !cuLaunchKernel || !cuCtxSynchronize) {
        fprintf(stderr, "missing CUDA driver symbol\n");
        return 1;
    }

    CUdevice dev;
    CUcontext ctx;
    CUmodule mod;
    CUfunction fn;
    uint64_t cycles = (uint64_t)seconds * 1410000000ULL;
    void *params[] = { &cycles };

    CHECK(cuInit(0));
    CHECK(cuDeviceGet(&dev, 0));
    CHECK(cuCtxCreate(&ctx, 0, dev));
    CHECK(cuModuleLoadData(&mod, ptx));
    CHECK(cuModuleGetFunction(&fn, mod, "spin_kernel"));
    printf("cuda-spin-start seconds=%d cycles=%llu\n", seconds, (unsigned long long)cycles);
    fflush(stdout);
    CHECK(cuLaunchKernel(fn, 1, 1, 1, 1, 1, 1, 0, NULL, params, NULL));
    CHECK(cuCtxSynchronize());
    printf("cuda-spin-done\n");
    return 0;
}
