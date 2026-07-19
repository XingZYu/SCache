// SPDX-License-Identifier: Apache-2.0
// SparkUrma: TCP control channel for endpoint discovery and metadata exchange.
// TCP is used ONLY for control messages — data always goes over URMA.

#include "urma_types.h"
#include <cstdio>
#include <cstring>
#include <cstdlib>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <stdexcept>
#include <sys/select.h>

namespace spark_urma {

// ---- EndpointDescriptor serialization ----
void EndpointDescriptor::to_network() {
    protocol_version = htonl(protocol_version);
    uasid = htonl(uasid);
    jetty_id = htonl(jetty_id);
    segment_address = htobe64(segment_address);
    segment_length = htobe64(segment_length);
    segment_token = htonl(segment_token);
    max_chunk_bytes = htonl(max_chunk_bytes);
    transport_mode = htonl(transport_mode);
}

void EndpointDescriptor::from_network() {
    protocol_version = ntohl(protocol_version);
    uasid = ntohl(uasid);
    jetty_id = ntohl(jetty_id);
    segment_address = be64toh(segment_address);
    segment_length = be64toh(segment_length);
    segment_token = ntohl(segment_token);
    max_chunk_bytes = ntohl(max_chunk_bytes);
    transport_mode = ntohl(transport_mode);
}

// ---- TCP Control Channel ----
class TcpControlChannel {
public:
    TcpControlChannel() = default;
    ~TcpControlChannel() { close(); }

    // Server: listen on port, accept one connection, exchange endpoints
    static TcpControlChannel accept(int port) {
        TcpControlChannel ch;
        ch.is_server_ = true;

        int s = socket(AF_INET, SOCK_STREAM, 0);
        if (s < 0) throw std::runtime_error("TCP socket() failed");

        int opt = 1;
        setsockopt(s, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

        struct sockaddr_in addr = {};
        addr.sin_family = AF_INET;
        addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        addr.sin_port = htons(static_cast<uint16_t>(port));
        if (bind(s, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr)) < 0) {
            ::close(s);
            throw std::runtime_error("TCP bind() failed");
        }
        if (listen(s, 1) < 0) {
            ::close(s);
            throw std::runtime_error("TCP listen() failed");
        }

        ch.fd_ = ::accept(s, nullptr, nullptr);
        ::close(s);
        if (ch.fd_ < 0) throw std::runtime_error("TCP accept() failed");
        return ch;
    }

    // Client: connect to server, exchange endpoints
    static TcpControlChannel connect(const std::string& host, int port) {
        TcpControlChannel ch;
        ch.is_server_ = false;

        // Convert hostname to connect
        struct sockaddr_in addr = {};
        addr.sin_family = AF_INET;
        addr.sin_port = htons(static_cast<uint16_t>(port));
        if (host == "127.0.0.1" || host == "localhost") {
            addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        } else {
            addr.sin_addr.s_addr = inet_addr(host.c_str());
            if (addr.sin_addr.s_addr == INADDR_NONE) {
                throw std::runtime_error("Invalid host: " + host);
            }
        }

        for (int retry = 0; retry < 200; retry++) {
            int s = socket(AF_INET, SOCK_STREAM, 0);
            if (s < 0) continue;
            if (::connect(s, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr)) == 0) {
                ch.fd_ = s;
                return ch;
            }
            ::close(s);
            usleep(5000);
        }
        throw std::runtime_error("TCP connect() timed out");
    }

    // Exchange endpoint descriptors
    void exchange(EndpointDescriptor& mine, EndpointDescriptor& peer) {
        // Send mine
        EndpointDescriptor net_mine = mine;
        net_mine.to_network();
        ssize_t w = write_all(&net_mine, sizeof(net_mine));
        if (w < 0) throw std::runtime_error("TCP write endpoint failed");

        // Receive peer
        ssize_t r = read_all(&peer, sizeof(peer));
        if (r < 0) throw std::runtime_error("TCP read endpoint failed");
        peer.from_network();

        // Protocol version check
        if (peer.protocol_version != PROTOCOL_VERSION) {
            throw std::runtime_error("Protocol version mismatch: got " +
                std::to_string(peer.protocol_version) + " expected " +
                std::to_string(PROTOCOL_VERSION));
        }
    }

    // Send a command (simple tagged message)
    void send_command(uint32_t cmd, const void* data, size_t len) {
        uint32_t net_cmd = htonl(cmd);
        uint32_t net_len = htonl(static_cast<uint32_t>(len));
        write_all(&net_cmd, sizeof(net_cmd));
        write_all(&net_len, sizeof(net_len));
        if (len > 0 && data) {
            write_all(data, len);
        }
    }

    // Receive a command
    uint32_t recv_command(std::vector<uint8_t>& data) {
        uint32_t cmd, len;
        read_all(&cmd, sizeof(cmd));
        read_all(&len, sizeof(len));
        cmd = ntohl(cmd);
        len = ntohl(len);
        data.resize(len);
        if (len > 0) {
            read_all(data.data(), len);
        }
        return cmd;
    }

    void close() {
        if (fd_ >= 0) {
            ::close(fd_);
            fd_ = -1;
        }
    }

    int fd() const { return fd_; }

    // Known commands
    static constexpr uint32_t CMD_READY = 1;
    static constexpr uint32_t CMD_DONE  = 2;
    static constexpr uint32_t CMD_ERROR = 3;

private:
    ssize_t write_all(const void* buf, size_t len) {
        size_t off = 0;
        while (off < len) {
            ssize_t n = ::write(fd_, static_cast<const char*>(buf) + off, len - off);
            if (n <= 0) return -1;
            off += static_cast<size_t>(n);
        }
        return static_cast<ssize_t>(len);
    }

    ssize_t read_all(void* buf, size_t len) {
        size_t off = 0;
        while (off < len) {
            ssize_t n = ::read(fd_, static_cast<char*>(buf) + off, len - off);
            if (n <= 0) return -1;
            off += static_cast<size_t>(n);
        }
        return static_cast<ssize_t>(len);
    }

    int fd_ = -1;
    bool is_server_ = false;
};

}  // namespace spark_urma
