#include "aeron_writer.hpp"
#include <cstdio>
#include <new>

AeronWriter::AeronWriter() {
  printf("Creating writer...\n");
  to_aeron_writer = new to_aeron(AERON_SS);
  op_buffer = (char *) malloc(1024*1024);
  t = (ToBUpdate *)malloc(sizeof(struct ToBUpdate));
  t->msg_header   = {sizeof(ToBUpdate), TOB_UPDATE, 1};
  printf("Created writer...\n");
}

uint64_t AeronWriter::get_current_ts_ns() {
    struct timespec t;
    clock_gettime(CLOCK_REALTIME, &t);
    return((t.tv_sec*1000000000L)+t.tv_nsec);
}

void AeronWriter::send_tob(t_tob_state *tob) {
  auto ts = get_current_ts_ns();
  t->receive_timestamp  = ts;
  t->exchange_timestamp = ts;
  t->bid_price          = tob->price;
  t->bid_qty            = 0;
  t->ask_price          = tob->price;
  t->ask_qty            = 0;
  t->instrument_id      = tob->id;
  t->exchange_id        = 0;
  to_aeron_writer->send_data((char*)t, sizeof(struct ToBUpdate));
}

AeronWriter *AeronWriter::new_instance() {
  return new(std::nothrow) AeronWriter;
}

extern "C" {

  void *new_instance( void ) {
    return AeronWriter::new_instance();
  }

  int send_tob(void *ptr, t_tob_state *tob) {
    try {
      AeronWriter *ref = reinterpret_cast<AeronWriter *>(ptr);
      ref->send_tob(tob);
    } catch(...) {
    }
    return 0;
  }
}
