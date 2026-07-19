// SPDX-License-Identifier: Apache-2.0
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <exception>
#include <memory>

#include "urma_transport.h"

using namespace spark_urma;

int main(int argc, char** argv) {
    try {
        const size_t operations = argc > 1 ? std::strtoull(argv[1], nullptr, 10) : 1100;
        if (operations < 1024) {
            std::fprintf(stderr, "FAIL operations must be at least 1024\n");
            return 1;
        }
        const size_t buffer_size = ((operations + 4095) / 4096) * 4096;
        std::unique_ptr<uint8_t, decltype(&std::free)> buffer(
            static_cast<uint8_t*>(std::aligned_alloc(4096, buffer_size)), &std::free);
        if (!buffer) {
            std::fprintf(stderr, "FAIL aligned buffer allocation\n");
            return 1;
        }
        TransportOptions options;
        options.device_name = "openurma0";
        options.wire_role = "connect";
        options.request_timeout_ms = 5000;

        UrmaTransport transport;
        transport.init(options);
        const RegisteredRegion local = transport.register_region(buffer.get(), buffer_size);
        transport.connect(transport.local_endpoint());

        for (size_t i = 0; i < operations; ++i) {
            RemoteRegion remote;
            remote.remote_address = local.remote_address + i;
            remote.length = 1;
            remote.token = local.token;
            buffer.get()[0] = static_cast<uint8_t>(i);
            const uint64_t request = transport.write(remote, buffer.get(), 1);
            const CompletionResult result = transport.wait(request, options.request_timeout_ms);
            if (result.status != 0 || result.bytes != 1) {
                std::fprintf(stderr, "FAIL operation=%zu status=%d bytes=%zu\n",
                             i, result.status, result.bytes);
                return 1;
            }
        }
        const auto active = transport.metrics().active_imports.load();
        const auto imported = transport.metrics().imported_regions.load();
        const auto unimported = transport.metrics().unimported_regions.load();
        const auto outstanding = transport.metrics().provider_outstanding_requests.load();
        std::printf("operations=%zu imported=%llu unimported=%llu active=%llu outstanding=%llu\n",
                    operations, (unsigned long long)imported, (unsigned long long)unimported,
                    (unsigned long long)active, (unsigned long long)outstanding);
        if (imported != operations || active != 1024 || unimported != operations - active ||
            outstanding != 0) {
            std::fprintf(stderr, "FAIL remote import cache did not plateau\n");
            return 1;
        }
        transport.close();
        if (transport.metrics().active_imports.load() != 0) {
            std::fprintf(stderr, "FAIL close did not release remote imports\n");
            return 1;
        }
        UrmaTransport::shutdown_process_runtime();
        std::puts("PASS remote import cache bounded LRU and close cleanup");
        return 0;
    } catch (const std::exception& error) {
        std::fprintf(stderr, "FAIL exception=%s\n", error.what());
        return 1;
    }
}
