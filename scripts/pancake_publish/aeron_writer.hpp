#include "to_aeron.hpp"
#include "aeron_types.hpp"
#include "ToBUpdate.h"

extern "C"

struct t_tob_state {
  int id = 0;
  double price = 0;
};  

class AeronWriter {

  to_aeron * to_aeron_writer;
  sbe::ToBUpdate *tob_sbe;
  char *op_buffer;

  public:
    AeronWriter();
    static AeronWriter *new_instance();
    void send_tob(t_tob_state *tob);
};
