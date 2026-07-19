#include <atomic>
#include <chrono>
#include <condition_variable>
#include <deque>
#include <functional>
#include <future>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#define private public
#include "urma_transport.h"
#undef private

#include <cstdio>
#include <cstdlib>

using namespace spark_urma;

static void require(bool condition, const char* message) {
    if (!condition) {
        std::fprintf(stderr, "FAIL %s\n", message);
        std::exit(1);
    }
}

static UrmaTransport::RequestRecord request(uint64_t id, size_t bytes) {
    UrmaTransport::RequestRecord value;
    value.request_id = id;
    value.bytes = bytes;
    value.state = RequestState::POSTED;
    return value;
}

int main() {
    UrmaTransport transport;

    // Test-only deterministic child-poster model.  The injection point lives in this
    // test translation unit and therefore cannot become part of libSparkUrma.so.
    // Child 1 is accepted by the provider, child 2 fails synchronously while posting,
    // and the already-posted child remains provider-owned until it is drained.
    auto post_failure_aggregate = request(10, 12288);
    post_failure_aggregate.is_aggregate = true;
    transport.requests_.emplace(10, post_failure_aggregate);
    const size_t fail_post_at = 2;
    for (size_t child_index = 1; child_index <= 3; ++child_index) {
        if (child_index == fail_post_at) {
            transport.finalize_locked(transport.requests_.at(10),
                                      RequestState::COMPLETED_FAILED,
                                      static_cast<int>(ErrorCode::ERR_COMPLETION), 0,
                                      "test-only injected child post failure at index 2");
            break;
        }
        const uint64_t child_id = 10 + child_index;
        auto child = request(child_id, 4096);
        child.provider_outstanding = true;
        transport.requests_.emplace(child_id, child);
        transport.requests_.at(10).children.push_back(child_id);
        transport.metrics_.record_provider_post(child_id);
    }
    require(transport.requests_.at(10).state == RequestState::COMPLETED_FAILED,
            "second child post failure fails aggregate");
    require(transport.requests_.at(11).provider_outstanding,
            "post failure must retain already-posted provider child");
    transport.provider_drained_locked(transport.requests_.at(11));
    require(transport.metrics_.provider_outstanding_requests.load() == 0,
            "already-posted child drains after aggregate post failure");

    // Provider drain releases remote-import ownership exactly once.
    auto remote_ref = request(12, 64);
    remote_ref.provider_outstanding = true;
    remote_ref.remote_segment = reinterpret_cast<urma_target_seg_t*>(0x1234);
    transport.imported_regions_.push_back(
        {RemoteRegion{}, remote_ref.remote_segment, 1, 1});
    transport.requests_.emplace(12, remote_ref);
    transport.metrics_.record_provider_post(12);
    transport.provider_drained_locked(transport.requests_.at(12));
    require(transport.imported_regions_.front().inflight == 0,
            "provider drain releases remote import reference");
    transport.provider_drained_locked(transport.requests_.at(12));
    require(transport.imported_regions_.front().inflight == 0,
            "remote import release is idempotent");
    transport.imported_regions_.clear();

    // Deterministic close models: a completion before the deadline permits cleanup;
    // a deadline with provider ownership still held must refuse cleanup.
    transport.requests_.emplace(20, request(20, 64));
    transport.requests_.at(20).provider_outstanding = true;
    transport.metrics_.record_provider_post(20);
    transport.provider_drained_locked(transport.requests_.at(20));
    transport.finalize_locked(transport.requests_.at(20), RequestState::COMPLETED_SUCCESS,
                              0, 64, "test-only close drain success");
    std::vector<std::string> close_success_errors;
    transport.cleanup_locked(close_success_errors);
    require(close_success_errors.empty(), "close drain success permits cleanup");

    transport.requests_.emplace(21, request(21, 64));
    transport.requests_.at(21).provider_outstanding = true;
    transport.metrics_.record_provider_post(21);
    transport.finalize_locked(transport.requests_.at(21),
                              RequestState::CANCELLED_PROVIDER_OUTSTANDING,
                              static_cast<int>(ErrorCode::ERR_CLOSED), 0,
                              "test-only close drain timeout");
    std::vector<std::string> close_timeout_errors;
    transport.cleanup_locked(close_timeout_errors);
    require(!close_timeout_errors.empty(), "close drain timeout refuses cleanup");
    require(transport.requests_.at(21).provider_outstanding,
            "close timeout preserves provider ownership");
    transport.provider_drained_locked(transport.requests_.at(21));

    // Deterministic middle-child completion failure must fail the aggregate.
    auto aggregate = request(100, 8192);
    aggregate.is_aggregate = true;
    aggregate.children = {101, 102};
    transport.requests_.emplace(100, aggregate);
    transport.requests_.emplace(101, request(101, 4096));
    transport.requests_.emplace(102, request(102, 4096));
    transport.finalize_locked(transport.requests_.at(101), RequestState::COMPLETED_SUCCESS, 0, 4096, "");
    transport.finalize_locked(transport.requests_.at(102), RequestState::COMPLETED_FAILED,
                              static_cast<int>(ErrorCode::ERR_COMPLETION), 0, "injected child failure");
    transport.refresh_aggregates_locked();
    require(transport.requests_.at(100).state == RequestState::COMPLETED_FAILED, "aggregate state");
    require(transport.requests_.at(100).completed_bytes == 4096, "aggregate completed bytes");

    // Terminal results are idempotent and independent of wait order.
    transport.requests_.emplace(201, request(201, 64));
    transport.requests_.emplace(202, request(202, 64));
    transport.finalize_locked(transport.requests_.at(201), RequestState::COMPLETED_SUCCESS, 0, 64, "");
    transport.finalize_locked(transport.requests_.at(202), RequestState::COMPLETED_SUCCESS, 0, 64, "");
    require(transport.result_locked(transport.requests_.at(202)).bytes == 64, "reverse result 202");
    require(transport.result_locked(transport.requests_.at(201)).bytes == 64, "reverse result 201");
    require(transport.result_locked(transport.requests_.at(202)).status == 0, "idempotent terminal result");

    bool unknown_failed = false;
    try {
        (void)transport.wait(999999, 1);
    } catch (const std::exception& error) {
        unknown_failed = std::string(error.what()).find("ERR_UNKNOWN_REQUEST") != std::string::npos;
    }
    require(unknown_failed, "unknown request must fail");
    require(transport.metrics_.unknown_waits.load() == 1, "unknown wait metric");
    transport.requests_.emplace(301, request(301, 64));
    transport.requests_.at(301).provider_outstanding = true;
    transport.metrics_.record_provider_post(301);
    transport.finalize_locked(transport.requests_.at(301), RequestState::TIMED_OUT_PROVIDER_OUTSTANDING,
                              static_cast<int>(ErrorCode::ERR_TIMEOUT), 0, "injected timeout");
    transport.finalize_locked(transport.requests_.at(301), RequestState::COMPLETED_SUCCESS, 0, 64,
                              "injected late completion");
    require(transport.requests_.at(301).state == RequestState::TIMED_OUT_PROVIDER_OUTSTANDING,
            "late completion must not reverse timeout");
    require(transport.metrics_.timed_out_requests.load() == 1, "timeout metric exactly once");
    require(transport.metrics_.provider_outstanding_requests.load() == 1,
            "timeout must preserve provider outstanding");
    transport.provider_drained_locked(transport.requests_.at(301));
    require(transport.requests_.at(301).state == RequestState::TIMED_OUT_DRAINED,
            "late completion drains without success");
    require(transport.metrics_.provider_outstanding_requests.load() == 0,
            "provider outstanding drained once");
    const uint64_t drained = transport.metrics_.provider_drained_requests.load();
    transport.provider_drained_locked(transport.requests_.at(301));
    require(transport.metrics_.provider_drained_requests.load() == drained,
            "provider drain idempotent");
    require(transport.wait(301, 1).status == static_cast<int>(ErrorCode::ERR_TIMEOUT),
            "wait-after-timeout preserves timeout");
    require(transport.wait(301, 1).status == static_cast<int>(ErrorCode::ERR_TIMEOUT),
            "repeated wait is idempotent");

    // A timed-out aggregate child deterministically fails the parent.
    auto timeout_aggregate = request(400, 12288);
    timeout_aggregate.is_aggregate = true;
    timeout_aggregate.children = {401, 402, 403};
    transport.requests_.emplace(400, timeout_aggregate);
    transport.requests_.emplace(401, request(401, 4096));
    transport.requests_.emplace(402, request(402, 4096));
    transport.requests_.emplace(403, request(403, 4096));
    transport.finalize_locked(transport.requests_.at(401), RequestState::COMPLETED_SUCCESS, 0, 4096, "");
    transport.finalize_locked(transport.requests_.at(402), RequestState::TIMED_OUT_DRAINED,
                              static_cast<int>(ErrorCode::ERR_TIMEOUT), 0, "injected child timeout");
    transport.refresh_aggregates_locked();
    require(transport.requests_.at(400).state == RequestState::COMPLETED_FAILED,
            "child timeout fails aggregate");
    require(transport.requests_.at(400).completed_bytes == 4096,
            "timeout aggregate completed bytes exclude failed children");

    // Cleanup must refuse to unregister memory while provider work is outstanding.
    transport.metrics_.provider_outstanding_requests.store(1);
    std::vector<std::string> cleanup_errors;
    transport.cleanup_locked(cleanup_errors);
    require(!cleanup_errors.empty(), "cleanup rejects provider outstanding");
    transport.metrics_.provider_outstanding_requests.store(0);

    // Bounded terminal cache remains safe beyond capacity.
    UrmaTransport cache_transport;
    for (uint64_t id = 1; id <= UrmaTransport::MAX_TERMINAL_RESULTS + 32; ++id) {
        cache_transport.requests_.emplace(id, request(id, 1));
        cache_transport.finalize_locked(cache_transport.requests_.at(id),
                                        RequestState::COMPLETED_SUCCESS, 0, 1, "");
    }
    require(cache_transport.terminal_order_.size() <= UrmaTransport::MAX_TERMINAL_RESULTS,
            "terminal order is bounded");
    require(cache_transport.requests_.size() <= UrmaTransport::MAX_TERMINAL_RESULTS,
            "terminal request cache is bounded");
    std::puts("PASS request_state_test child_post_failure_2 close_drain_success "
              "close_drain_timeout aggregate_failure child_timeout reverse_results "
              "unknown_request wait_after_timeout provider_drain remote_import_ref "
              "cleanup_guard terminal_cache");
    return 0;
}
