#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <thread>

#include <cuda_runtime.h>

__global__ void spin_kernel(unsigned long long cycles) {
    unsigned long long start = clock64();
    while (clock64() - start < cycles) {
    }
}

int main(int argc, char** argv) {
    int device = 0;
    if (argc > 1) {
        device = std::atoi(argv[1]);
    }
    cudaError_t err = cudaSetDevice(device);
    if (err != cudaSuccess) {
        std::fprintf(stderr, "cudaSetDevice(%d) failed: %s\n", device, cudaGetErrorString(err));
        return 1;
    }
    std::fprintf(stderr, "cuda-spin started on device %d\n", device);
    while (true) {
        spin_kernel<<<1, 256>>>(50000000ULL);
        err = cudaDeviceSynchronize();
        if (err != cudaSuccess) {
            std::fprintf(stderr, "cudaDeviceSynchronize failed: %s\n", cudaGetErrorString(err));
            return 2;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
}
