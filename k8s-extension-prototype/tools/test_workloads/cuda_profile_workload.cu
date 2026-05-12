#include <cuda_runtime.h>

#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>

__global__ void profile_kernel(const float* a, const float* b, float* c, int n) {
    int idx = blockIdx.x * blockDim.x + threadIdx.x;
    if (idx >= n) {
        return;
    }
    float x = a[idx];
    float y = b[idx];
    #pragma unroll 16
    for (int i = 0; i < 256; ++i) {
        x = x * 1.000001f + y * 0.999999f + 0.000001f;
        y = y * 0.999997f + x * 0.000003f;
    }
    c[idx] = x + y;
}

static void check(cudaError_t err, const char* what) {
    if (err != cudaSuccess) {
        std::fprintf(stderr, "%s failed: %s\n", what, cudaGetErrorString(err));
        std::exit(2);
    }
}

static const char* arg_value(int argc, char** argv, const char* key, const char* fallback) {
    for (int i = 1; i + 1 < argc; ++i) {
        if (std::strcmp(argv[i], key) == 0) {
            return argv[i + 1];
        }
    }
    return fallback;
}

int main(int argc, char** argv) {
    const char* workload = arg_value(argc, argv, "--workload", "resnet50");
    const char* profile = arg_value(argc, argv, "--profile", "3g");
    const char* batch = arg_value(argc, argv, "--batch", "4");
    int seconds = std::atoi(arg_value(argc, argv, "--seconds", "12"));
    if (seconds <= 0) {
        seconds = 12;
    }

    int device_count = 0;
    check(cudaGetDeviceCount(&device_count), "cudaGetDeviceCount");
    if (device_count <= 0) {
        std::fprintf(stderr, "no CUDA devices visible\n");
        return 3;
    }

    cudaDeviceProp prop{};
    check(cudaGetDeviceProperties(&prop, 0), "cudaGetDeviceProperties");
    std::printf("profile_workload_start workload=%s profile=%s batch=%s seconds=%d\n",
                workload, profile, batch, seconds);
    std::printf("cuda_device name=%s multiprocessors=%d global_mem_mib=%zu\n",
                prop.name, prop.multiProcessorCount, prop.totalGlobalMem / 1024 / 1024);

    const int n = 8 * 1024 * 1024;
    const size_t bytes = static_cast<size_t>(n) * sizeof(float);
    float* a = nullptr;
    float* b = nullptr;
    float* c = nullptr;
    check(cudaMalloc(&a, bytes), "cudaMalloc(a)");
    check(cudaMalloc(&b, bytes), "cudaMalloc(b)");
    check(cudaMalloc(&c, bytes), "cudaMalloc(c)");
    check(cudaMemset(a, 1, bytes), "cudaMemset(a)");
    check(cudaMemset(b, 2, bytes), "cudaMemset(b)");

    const int threads = 256;
    const int blocks = (n + threads - 1) / threads;
    auto started = std::chrono::steady_clock::now();
    int iterations = 0;
    while (true) {
        profile_kernel<<<blocks, threads>>>(a, b, c, n);
        check(cudaGetLastError(), "profile_kernel launch");
        ++iterations;
        if (iterations % 8 == 0) {
            check(cudaDeviceSynchronize(), "cudaDeviceSynchronize");
            auto now = std::chrono::steady_clock::now();
            double elapsed = std::chrono::duration<double>(now - started).count();
            std::printf("profile_workload_progress iterations=%d elapsed_s=%.3f\n",
                        iterations, elapsed);
            std::fflush(stdout);
            if (elapsed >= seconds) {
                break;
            }
        }
    }
    check(cudaDeviceSynchronize(), "cudaDeviceSynchronize(final)");
    auto finished = std::chrono::steady_clock::now();
    double elapsed = std::chrono::duration<double>(finished - started).count();
    std::printf("profile_workload_done workload=%s profile=%s batch=%s iterations=%d elapsed_s=%.3f\n",
                workload, profile, batch, iterations, elapsed);
    std::fflush(stdout);

    cudaFree(a);
    cudaFree(b);
    cudaFree(c);
    return 0;
}
