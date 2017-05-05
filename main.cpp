#include <stdio.h>
#include <unistd.h>
#include <time.h>
#include <zmq.hpp>

// extend zmq library
#if defined __GNUC__
#define likely(x) __builtin_expect ((x), 1)
#define unlikely(x) __builtin_expect ((x), 0)
#else
#define likely(x) (x)
#define unlikely(x) (x)
#endif

char *xreq_host      = "tcp://*:1234";
char *xrep_host      = "tcp://*:5678";
char *dump_file_name = "zmq_xproxy.dump";
int  is_debug        = 0;

FILE *dump_file = NULL;

void print_usage()
{
    printf("zmq_xproxy - Proxy server on for REQ/REP\n");
    printf("[cid, null, sid, req ...] <-x-> [sid, null, cid, rep ...]\n");
    printf("\n");
    printf("Usage: zmq_xproxy [-k value]\n");
    printf("  -m to set service mounting host,   default tcp://*:1234\n");
    printf("  -a to set service accessing host,  default tcp://*:5678\n");
    printf("  -f to set path of debug data file, default zmq_xproxy.dump\n");
    printf("  -d to print debug data\n");
    printf("  -h to print this help information\n");
    printf("NOTE: use ZMQ_DEALER instead of ZMQ_REP to connect mounting host.");
    printf("\n");
}

void init_arguments(int argc, char *argv[])
{
    int opt;
    while((opt = getopt(argc, argv, "m:a:f:dh")) != -1) {
        switch(opt) {
        case 'm':
            xreq_host = optarg;
            break;
        case 'a':
            xrep_host = optarg;
            break;
        case 'f':
            dump_file_name = optarg;
            break;
        case 'd':
            is_debug = 1;
            break;
        default:
            print_usage();
            exit(0);
        }
    }
}

void xcapture(zmq::message_t *msg_, bool is_m2a)
{
    if (is_debug && dump_file) {
        time_t now;
        struct tm *tm_ptr;
        now = time(NULL);
        tm_ptr = localtime(&now);
        fprintf(dump_file, "%04d-%02d-%02d %02d:%02d:%02d %s ",
            tm_ptr->tm_year+1900, tm_ptr->tm_mon+1, tm_ptr->tm_mday,
            tm_ptr->tm_hour, tm_ptr->tm_min, tm_ptr->tm_sec, is_m2a ? "->" : "<-");
        for (unsigned int i=0; i<msg_->size(); i++) {
            fprintf(dump_file, "%02x ", (unsigned char)(*((const char*)(msg_->data())+i)));
        }
        fprintf(dump_file, "[");
        fwrite(msg_->data(), 1, msg_->size(), dump_file);
        fprintf(dump_file, "]\n");
        fflush(dump_file);
    }
}

int xforward(zmq::socket_t *from_, zmq::socket_t *to_, bool is_m2a)
{
    std::vector<zmq::message_t *> msg_array;
    int more;
    size_t moresz;
    while (true) {
        zmq::message_t *msg_ = new zmq::message_t();
        from_->recv(msg_);
        msg_array.push_back(msg_);
        moresz = sizeof more;
        from_->getsockopt(ZMQ_RCVMORE, &more, &moresz);
        xcapture(msg_, is_m2a);
        if (more == 0) break;
    }

    // swap the request client and reply server socket IDs
    int msg_size = msg_array.size();
    if (msg_size > 2 ) {
        std::swap(msg_array[0], msg_array[2]);
    }

    // forward all messages
    for (int i = 0; i<msg_size; i++) {
        to_->send(*(msg_array[i]), i<msg_size-1 ? ZMQ_SNDMORE : 0);
        delete (msg_array[i]);
    }

    return 0;
}

// proxy router - router messages
int xproxy(zmq::socket_t *frontend_, zmq::socket_t *backend_)
{
    int rc;

    //  The algorithm below assumes ratio of requests and replies processed
    //  under full load to be 1:1.

    zmq::pollitem_t items [] = {
        { *frontend_, 0, ZMQ_POLLIN, 0 },
        { *backend_, 0, ZMQ_POLLIN, 0 }
    };
    zmq::pollitem_t itemsout [] = {
        { *frontend_, 0, ZMQ_POLLOUT, 0 },
        { *backend_, 0, ZMQ_POLLOUT, 0 }
    };

    while (true) {
        //  Wait while there are either requests or replies to process.
        rc = zmq::poll(items, 2, -1);
        if (unlikely(rc < 0)) return -1;

        //  Get the pollout separately because when combining this with pollin it maxes the CPU
        //  because pollout shall most of the time return directly.
        //  POLLOUT is only checked when frontend and backend sockets are not the same.
        if (frontend_ != backend_) {
            rc = zmq::poll(itemsout, 2, 0);
            if (unlikely(rc < 0)) return -1;
        }

        //  Process a request
        if (items [0].revents & ZMQ_POLLIN &&  (frontend_ == backend_ || itemsout [1].revents & ZMQ_POLLOUT)) {
            rc = xforward(frontend_, backend_, false);
            if (unlikely(rc < 0)) return -1;
        }
        //  Process a reply
        if (frontend_ != backend_ &&  items [1].revents & ZMQ_POLLIN &&  itemsout [0].revents & ZMQ_POLLOUT) {
            rc = xforward(backend_, frontend_, true);
            if (unlikely(rc < 0)) return -1;
        }
    }
    return 0;
}

int main(int argc, char *argv[])
{
    init_arguments(argc, argv);

    if (is_debug) {
        dump_file = fopen(dump_file_name, "ab");
    }

    zmq::context_t context(1);
    zmq::socket_t frontend(context, ZMQ_ROUTER);
    zmq::socket_t backend(context, ZMQ_ROUTER);
    frontend.bind(xrep_host);
    backend.bind(xreq_host);
    printf("Start x-proxy service...\n");

    xproxy(&frontend, &backend);

    if (is_debug && dump_file) {
        fclose(dump_file);
    }

    return 0;
}
