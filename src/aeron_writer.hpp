#include "to_aeron.hpp"
#include "aeron_types.hpp"

extern "C"

struct t_tob_state {
  int id = 0;
  double price = 0;
};  

class AeronWriter {
  private:
    to_aeron * to_aeron_writer;
    struct ToBUpdate *t;
    char *op_buffer;

    uint64_t get_current_ts_ns();

  public:
    AeronWriter();
    static AeronWriter *new_instance();
    void send_tob(t_tob_state *tob);
};
