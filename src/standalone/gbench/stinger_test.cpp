#include <algorithm>
#ifndef B64
#define B64
#endif

#include "new_type.h"
#include "batching.h"
#include "util.h"
#include "graph_view.h"
#include "diff_bfs.h"


typekv_t* typekv;
struct stinger* S;


int _part_count;
index_t residue = 0;
int THD_COUNT = 9;
vid_t _global_vcount = 0;
int _dir = 0;//undirected
int _source = 0;//text
int _num_sources = 1;//only one data-source
int _format = 0;
int64_t _arg = 1;//algo specific argument
int _numtasks = 0, _rank = 0;
int _numlogs = 1;

index_t  BATCH_SIZE = (1L << 16);//edge batching in edge log
index_t  BATCH_MASK =  0xFFFF;
index_t  BATCH_TIMEOUT = (-1L);

#ifdef B64
propid_t INVALID_PID = 0xFFFF;
tid_t    INVALID_TID  = 0xFFFFFFFF;
sid_t    INVALID_SID  = 0xFFFFFFFFFFFFFFFF;
#else 
propid_t INVALID_PID = 0xFF;
tid_t    INVALID_TID  = 0xFF;
sid_t    INVALID_SID  = 0xFFFFFFFF;
#endif

//run adjacency store creation here.
status_t create_adjacency_snapshot(ubatch_t* ubatch)
{
    status_t status  = ubatch->create_mbatch();
    if (0 == ubatch->reader_archive) {
        assert(0);
    }

    #pragma omp parallel num_threads(THD_COUNT)
    {
    vsnapshot_t* startv = ubatch->get_archived_vsnapshot();
    vsnapshot_t* endv   = ubatch->get_to_vsnapshot();
    blog_t* blog = ubatch->blog;

    do {
        if (startv) {
            startv = startv->get_prev();
        } else {
            startv = ubatch->get_oldest_vsnapshot();
        }

        
        int updates_count = 0;
        int deletes_count = 0;
        vid_t src, dst;
        index_t tail, marker, index;
        edge_t* edge, *edges;
        vid_t v_count = typekv->get_type_vcount(0);

        edges = blog->blog_beg;
        tail = startv->tail;
        marker = startv->marker;
        #pragma omp for
        for (index_t i = tail; i < marker; ++i) {
            index = (i & blog->blog_mask);
            edge = (edge_t*)((char*)edges + index*ubatch->edge_size);
            src = edge->src_id;
            dst = TO_SID(edge->get_dst());
            
            assert(TO_SID(src) < v_count);
            assert(dst < v_count);
            if (IS_DEL(src)) {//deletion case
                stinger_remove_edge(S, 0, TO_VID(src), dst);
                stinger_remove_edge(S, 0, dst, TO_VID(src));
            } else {
                stinger_insert_edge(S, 0, src, dst, 1, updates_count);
                stinger_insert_edge(S, 0, dst, src, 1, updates_count);
            }
            ++updates_count;
        }
    } while (startv != endv);
    }

    
    //updating is required
    ubatch->update_marker();
    //cout << endv->id << endl << endl; 

    return status;
}

int stinger_test(vid_t v_count, const string& idir, const string& odir)
{
    omp_set_num_threads(THD_COUNT);
    //Initialize the system. 
    //Return ubatch pointer here.
    S = stinger_new();
    int64_t* off = (int64_t*)calloc(v_count, sizeof(int64_t));
    int64_t* ind = 0;
    int64_t* weight = 0;

    stinger_set_initial_edges (S, v_count, 0, off, ind, weight, NULL, NULL, -2);

    ubatch_t* ubatch = new ubatch_t(sizeof(edge_t),  1);
    ubatch->alloc_edgelog(20); // 1 Million edges
    ubatch->reg_archiving();
    
    //create typekv as it handles the mapping for vertex id (string to vid_t)
    typekv = new typekv_t;
    typekv->manual_setup(v_count, true, "gtype");
    
    //If system support adjacency store snapshot, create thread for index creation
    // Run analytics in separte thread. If adjacency store is non-snapshot, do indexing and analytics in seq.
    index_t slide_sz = BATCH_SIZE;
    gview_t* sstreamh = reg_sstream_view(ubatch, v_count, stream_serial_bfs_del<dst_id_t>, C_THREAD, slide_sz);
    
    //perform micro batching here using ubatch pointer
    int64_t flags = 0;
    if (_source == 1) {
        flags = SOURCE_BINARY;
    }
    index_t total_edges = add_edges_from_dir<dst_id_t>(idir, ubatch, flags);
    ubatch->set_total_edges(total_edges);
    
    //Wait for threads to complete
    void* ret;
    pthread_join(sstreamh->thread, &ret);

    return 0;
}

void print_usage() 
{
    string help = "./exe options.\n";
    help += " --help -h: This message.\n";
    help += " --vcount -v: Vertex count\n";
    help += " --idir -i: input directory\n";
    help += " --odir -o: output directory. This option will also persist the edge log.\n";
    //help += " --category -c: 0 for single stream graphs. Default: 0\n";
    help += " --job -j: job number. Default: 0\n";
    help += " --threadcount --t: Thread count. Default: Cores in your system - 1\n";
    help += " --direction -d: Direction, 0 for undirected, 1 for directed, 2 for unidirected. Default: 0(undirected)\n";
    help += " --source  -s: Data source. 0 for text files, 1 for binary files. Default: text files\n";
    help += " --batch-size or -b: Batch size and Slide Size (must be power of 2).\n";

    cout << help << endl;
}


int main(int argc, char* argv[])
{
    const struct option longopts[] =
    {
        {"vcount",    required_argument,  0, 'v'},
        {"help",      no_argument,        0, 'h'},
        {"idir",      required_argument,  0, 'i'},
        {"odir",      required_argument,  0, 'o'},
        {"category",   required_argument,  0, 'c'},
        {"job",       required_argument,  0, 'j'},
        {"batch-size",   required_argument,  0, 'b'},
        {"threadcount",  required_argument,  0, 't'},
        {"direction",  required_argument,  0, 'd'},
        {"source",  required_argument,  0, 's'},
        {"num-datasource",  required_argument,  0, 'n'},
        {"arg",  required_argument,  0, 'a'},
        {0,			  0,				  0,  0},
    };

	int o;
	int index = 0;
	string idir, odir;
    int category = 0;
    int job = 0;
    vid_t global_vcount = 0;
    _part_count = 1;
	THD_COUNT = omp_get_max_threads()-1;// - 3;
    
	while ((o = getopt_long(argc, argv, "i:j:o:t:b:v:d:s:n:a:h", longopts, &index)) != -1) {
		switch(o) {
			case 'v':
				#ifdef B64
                sscanf(optarg, "%ld", &global_vcount);
				#elif B32
                sscanf(optarg, "%d", &global_vcount);
				#endif
				break;
			case 'h':
				print_usage();
                return 0;
                break;
			case 'i':
				idir = optarg;
				break;
			case 'a':
				_arg = atoi(optarg);
				break;
            case 'j':
                job = atoi(optarg);
				break;
			case 'o':
				odir = optarg;
				break;
            case 't':
                //Thread thing
                THD_COUNT = atoi(optarg);
                break;
            case 'd':
                sscanf(optarg, "%d", &_dir);
                assert(_dir == 0);//supports only undirected graphs
                break;
            case 's':
                sscanf(optarg, "%d", &_source);
                break;
            case 'b':
                sscanf(optarg, "%ld", &BATCH_SIZE);
                BATCH_MASK = BATCH_SIZE - 1;
                break;
            case 'n':
                sscanf(optarg, "%d", &_num_sources);
                break;
			default:
                cout << "invalid input " << endl;
                print_usage();
                return 1;
		}
	}

    cout << "input dir = " << idir << endl;
    cout << "output dir = " << odir << endl;
    cout << "Threads Count = " << THD_COUNT << endl;
    cout << "Global vcount = " << global_vcount << endl;
    cout << "batch-size = " << BATCH_SIZE << "(must be power of two)" <<endl;
    switch (job) {
        case 0:
        stinger_test(global_vcount, idir, odir);
            break;
        default:
            break;
    }
    return 0;
}
