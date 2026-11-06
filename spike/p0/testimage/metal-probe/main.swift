// metal-probe opens the default Metal device and runs a vector add on the GPU.
// Exit 0 means the GPU was usable from inside the pod sandbox.
import Metal

guard let dev = MTLCreateSystemDefaultDevice() else {
    print("FAIL: no Metal device (sandbox denied GPU?)")
    exit(2)
}
print("device: \(dev.name) unified=\(dev.hasUnifiedMemory) maxWorkingSet=\(dev.recommendedMaxWorkingSetSize)")

let src = """
#include <metal_stdlib>
kernel void add(device const float* a [[buffer(0)]], device const float* b [[buffer(1)]],
                device float* out [[buffer(2)]], uint i [[thread_position_in_grid]]) { out[i] = a[i] + b[i]; }
"""
do {
    let lib = try dev.makeLibrary(source: src, options: nil)
    let pso = try dev.makeComputePipelineState(function: lib.makeFunction(name: "add")!)
    let n = 1 << 20
    let a = [Float](repeating: 1.5, count: n), b = [Float](repeating: 2.25, count: n)
    let len = n * MemoryLayout<Float>.stride
    let ba = dev.makeBuffer(bytes: a, length: len)!, bb = dev.makeBuffer(bytes: b, length: len)!
    let bo = dev.makeBuffer(length: len)!
    let q = dev.makeCommandQueue()!, cb = q.makeCommandBuffer()!, enc = cb.makeComputeCommandEncoder()!
    enc.setComputePipelineState(pso)
    enc.setBuffer(ba, offset: 0, index: 0); enc.setBuffer(bb, offset: 0, index: 1); enc.setBuffer(bo, offset: 0, index: 2)
    enc.dispatchThreads(MTLSize(width: n, height: 1, depth: 1),
                        threadsPerThreadgroup: MTLSize(width: pso.maxTotalThreadsPerThreadgroup, height: 1, depth: 1))
    enc.endEncoding(); cb.commit(); cb.waitUntilCompleted()
    let out = bo.contents().bindMemory(to: Float.self, capacity: n)
    let ok = (0..<n).allSatisfy { out[$0] == 3.75 }
    print("vector add of \(n) floats on GPU: \(ok ? "OK" : "WRONG") gpuTime=\(String(format: "%.3f", (cb.gpuEndTime - cb.gpuStartTime) * 1000))ms")
    exit(ok ? 0 : 1)
} catch {
    print("FAIL: \(error)")
    exit(3)
}
