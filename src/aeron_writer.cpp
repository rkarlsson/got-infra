#include "aeron_writer.hpp"
#include <cstdio>
#include <new>

AeronWriter::AeronWriter() {
  printf("Creating writer...\n");
  to_aeron_writer = new to_aeron(AERON_SS);
  op_buffer = (char *) malloc(1024*1024);
  tob_sbe = new sbe::ToBUpdate();
  tob_sbe->wrapAndApplyHeader(&op_buffer[0], 0, 1024*1024);
  printf("Created writer...\n");
}

void AeronWriter::send_tob(t_tob_state *tob) {
  tob_sbe->receive_timestamp(0);
  tob_sbe->exchange_timestamp(0);
  tob_sbe->bid_price(tob->price);
  tob_sbe->bid_qty(0);
  tob_sbe->ask_price(tob->price);
  tob_sbe->ask_qty(0);
  tob_sbe->instrument_id(tob->id);
  tob_sbe->exchange_id(0);
  to_aeron_writer->send_data(&op_buffer[0], tob_sbe->sbePosition());
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
