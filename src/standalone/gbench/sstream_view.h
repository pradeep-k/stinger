#pragma once

//---stinger specific---
#define _LARGEFILE64_SOURCE 1
#define _FILE_OFFSET_BITS 64

#include "stinger_core/stinger_atomics.h"
#include "stinger_utils/stinger_utils.h"
#include "stinger_core/stinger.h"
#include "stinger_utils/timer.h"
#include "stinger_core/xmalloc.h"
//----- stinger specific-----//

#include "view_interface.h"

class sstream_t : public gview_t {
 public: 
    struct stinger *pgraph;

    
 public:
    virtual degree_t get_nebrs_out(vid_t vid, nebr_reader_t& header) {
        size_t d;
        degree_t degree = get_degree_out(vid);
        header.adjust_size(degree, header.T_size);
        stinger_gather_typed_successors (pgraph, 0, vid, &d, (int64_t*)header.ptr, degree);
        assert(degree == d);
        return degree;
    }
    virtual degree_t get_nebrs_in (vid_t vid, nebr_reader_t& header) {
        return get_nebrs_out(vid, header);
    }
    virtual degree_t get_degree_out(vid_t vid) {
        return stinger_outdegree (pgraph, vid);
    }
    virtual degree_t get_degree_in (vid_t vid) {
        return stinger_outdegree (pgraph, vid);
    }
    
    virtual status_t    update_view() {
        return update_view_help();
    }
    void init_view(ubatch_t* a_ubatch, vid_t a_vcount, index_t a_flag, index_t slide_sz1) {
        gview_t::init_view(a_ubatch, a_vcount, a_flag, slide_sz1);
        
        pgraph = 0; //TODO
    }
    
   
    inline sstream_t() {
        pgraph = 0; //TODO
    } 
    inline virtual ~sstream_t() {
        //We may need to free some internal memory.XXX
    } 
};

