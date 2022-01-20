
#include <omp.h>
#include <iostream>
#include <getopt.h>
#include <stdlib.h>
#include <algorithm>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <dirent.h>
#include "new_type.h"

using std::cout;
using std::endl;

#define no_argument 0
#define required_argument 1 
#define optional_argument 2

int _part_count;
index_t residue = 0;
int THD_COUNT = 2;
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

off_t fsize(const string& fname)
{
    struct stat st;
    if (0 == stat(fname.c_str(), &st)) {
        return st.st_size;
    }
    perror("stat issue");
    return -1L;
}

off_t fsize(int fd)
{
    struct stat st;
    if (0 == fstat(fd, &st)) {
        return st.st_size;
    }
    perror("stat issue");
    return -1L;
}

off_t fsize_dir(const string& idir)
{
    struct dirent *ptr;
    DIR *dir;
    string filename;
        
    index_t size = 0;
    index_t total_size = 0;
    

    //allocate accuately
    dir = opendir(idir.c_str());
    
    while (NULL != (ptr = readdir(dir))) {
        if (ptr->d_name[0] == '.') continue;
        filename = idir + "/" + string(ptr->d_name);
        size = fsize(filename);
        total_size += size;
    }
    closedir(dir);
    return total_size;
}

void plain_test(vid_t v_count, const string& idir, const string& odir);

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
    help += " --residue or -r: Various meanings.\n";

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
        {"residue",   required_argument,  0, 'r'},
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
    string queryfile;
    int category = 0;
    int job = 0;
    vid_t global_vcount = 0;
    _part_count = 1;
	THD_COUNT = omp_get_max_threads()-1;// - 3;
    
	while ((o = getopt_long(argc, argv, "i:j:o:t:r:v:d:s:n:a:h", longopts, &index)) != -1) {
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
                break;
            case 's':
                sscanf(optarg, "%d", &_source);
                break;
            case 'r':
                sscanf(optarg, "%ld", &residue);
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
    if (0 == _rank) {
        cout << "input dir = " << idir << endl;
        cout << "output dir = " << odir << endl;
        cout << "Threads Count = " << THD_COUNT << endl;
        cout << "Global vcount = " << global_vcount << endl;
        cout << "residue (multi-purpose) value) = " << residue << endl;
    }
    switch (job) {
        case 0:
        plain_test(global_vcount, idir, odir);
            break;
        default:
            break;
    }
    return 0;
}

