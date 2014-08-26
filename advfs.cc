/* Creates a compressed image, given a file as an argument.
 * (c)1999 Paul `Rusty' Russell.  GPL.
 *
 * CHANGELOG:
 *
 * * Wed, 02 Aug 2006 02:01:42 +0200 Eduard Bloch <blade@debian.org>
 * - cleanup, fixed memory leak in doLocalCompression with best compression
 * - simplified the control flow a lot, it was overdesigned. Kept one ring
 *   buffer for data passing and aside buffers for temp. data storage.
 *
 * * Mon, 29 Aug 2005 15:20:10 CEST 2005 Eduard Bloch <blade@debian.org>
 * - a complete overhaul, rewrote most parts using STL and C++ features,
 *   portable data types etc.pp.
 * - using multiple threads, input/output buffers, various strategies for
 *   temporary data storage, options parsing with getopt, and: network enabled
 *   server/client modes, distributing the compression jobs to many nodes
 *
 * * Mon Feb 28 20:45:14 CET 2005 Sebastian Schmidt <yath@yath.eu.org>
 * - Added -t command line option to use a temporary file for the compressed
 *   blocks instead of saving it in cb_list.
 * - Added information message before the write of the compressed image.
 * * Sat Deb 14 22:49:20 CEST 2004 Christian Leber
 * - Changed to work with the advancecomp packages, so that it's
 *   better algorithms may be used
 * * Sun Okt 26 01:05:29 CEST 2003 Klaus Knopper
 * - Changed format of index pointers to network byte order
 * * Sat Sep 29 2001 Klaus Knopper <knopper@knopper.net>
 * - changed compression to Z_BEST_COMPRESSION,
 * * Sat Jun 17 2000 Klaus Knopper <knopper@knopper.net>
 * - Support for reading file from stdin,
 * - Changed Preamble.
 * * Sat Jul 28 2001 Klaus Knopper <knopper@knopper.net>
 * - cleanup and gcc 2.96 / glibc checking
 */


#define _FILE_OFFSET_BITS 64

#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <pthread.h>
#include <time.h>
#include <endian.h>
#include <fcntl.h>
#include <zlib.h>
#include "cloop.h"
#include "portable.h"
#include "pngex.h"
//#include "utility.h"
#include "compress.h"
#include "siglock.h"

#ifndef __OPTIMIZE__
#define __OPTIMIZE__
#endif

#include <netinet/in.h>
#include <netinet/tcp.h>
#include <netdb.h>
#include <sys/types.h>
#include <sys/socket.h>
#if defined(__linux__)
#include <sys/socketvar.h>
#endif
#include "lib/mng.h"
#include "lib/endianrw.h"

// for server
#include <unistd.h>
#include <errno.h>
#include <string.h>
#include <arpa/inet.h>
#include <sys/wait.h>
#include <signal.h>

#include <iostream>
#include <iomanip>
#include <cstdio>
#include <string>
#include <list>
#include <map>
#include <vector>
#include <limits>

using namespace std;

#define DEBUG(x)
//#define DEBUG(x) cerr << x << endl;

//#define MAX_KMALLOC_SIZE 2L<<17

#define CLOOP_PREAMBLE "#!/bin/sh\n" "#V2.0 Format\n" "modprobe cloop file=$0 && mount -r -t iso9660 /dev/cloop $1\n" "exit $?\n"

#define MAXLEN(bs) ((bs) + (bs)/1000 + 12)

int maxlen(0);

#ifdef __CYGWIN__
typedef uint64_t loff_t;
#endif

# if defined(linux) || defined(__linux__)
#include <asm/byteorder.h>
#define ENSURE64UINT(x) __cpu_to_be64(x)

#else // not linux

#ifndef be64toh
#if BYTE_ORDER == LITTLE_ENDIAN
static __inline __uint64_t
__bswap64(__uint64_t _x)
{

        return ((_x >> 56) | ((_x >> 40) & 0xff00) | ((_x >> 24) & 0xff0000) |
            ((_x >> 8) & 0xff000000) | ((_x << 8) & ((__uint64_t)0xff << 32)) |
            ((_x << 24) & ((__uint64_t)0xff << 40)) |
            ((_x << 40) & ((__uint64_t)0xff << 48)) | ((_x << 56)));
}
#define be64toh(x)      __bswap64(x)
#else // BIG ENDIAN
#define be64toh(x) x
#endif
#endif /* !be64toh */
#define ENSURE64UINT be64toh

#endif // linux


#define die(msg) { cerr << "ERROR: " << msg << ". Exiting..."<<endl;  exit(1); }

#ifndef MSG_WAITALL
#define MSG_WAITALL 0
#endif

#ifndef MSG_NOSIGNAL
#define MSG_NOSIGNAL 0
#endif

// some globals, easier option passing
int defport=3103;
int ret=0;
unsigned long blocksize=0;
unsigned long expected_blocks=0;
//unsigned long numblocks=0;
int method=Z_BEST_COMPRESSION;
const int maxalg=11;
unsigned int levelcount[maxalg];
bool be_verbose(false), be_quiet(false);

#define TOFILE 0
#define TOTEMPFILE 1
#define TOMEM 2
int targetkind(TOFILE);
FILE *targetfh(NULL), *datafh(NULL), *tempfh(NULL);

bool reuse_as_tempfile(false);

int workThreads=3;
vector<char *> hostpool;

vector<uint64_t> lengths;
vector<char *> blocks;

pthread_mutex_t mainlock = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t maincond = PTHREAD_COND_INITIALIZER;
#define lock pthread_mutex_lock( &mainlock )
#define unlock pthread_mutex_unlock( &mainlock )
#define doSleep pthread_cond_wait(&maincond, &mainlock)
#define doAwake pthread_cond_broadcast(&maincond)

class compressItem;
compressItem *pool;
int poolsize(0);
int posAdd(0);
int posFetch(0);

bool terminateAll=false;

// job size
unsigned long jobsize = 32;

int in(-1);

int start_server(int port);
int setup_connection(char *peer);

/* cludge
FILE * split_fopen( char *path, char *mode);
int split_fwrite(void *ptr, size_t nmemb);
uint64_t chunk_size=0;
*/

class compressItem {
    public:

        // those are the only interesting unique attributes
        int best;
        unsigned long compLen;
#define STOPMARK -2
#define SDIRTY -1
#define SFRESH 0
#define SRESERVED 1
#define SCOMPRESSED 2
        int state;

        char *inBuf, *outBuf;

        compressItem() : state(SDIRTY) {
            maxlen=MAXLEN(blocksize); // is global, though
            inBuf  =(char *) malloc(blocksize);
            outBuf=(char *) malloc(maxlen);
        };

        void set_size (int id, int size)
        {
            //uncompLen=size;
            // if incomplete, add padding
        };

        ~compressItem() { // don't care, cleared at app exit
            // deleted by compress methods or by others
            //if(uncompBuf) delete[] uncompBuf;
            //if(compBuf) delete[] compBuf;
        }

        bool doRemoteCompression(int method, int con) {
            DEBUG("sending data");
            if(send(con, inBuf, blocksize, MSG_NOSIGNAL) == -1) {
                DEBUG("data sending failed");
                return false;
            }
            DEBUG("awaiting compressed data, should come back before TCP timeouts");
            uint32_t rhead[2];
            int l=recv(con, rhead, sizeof(rhead), MSG_WAITALL | MSG_NOSIGNAL);
            if(l<1) return false;
            
            compLen=ntohl(rhead[0]);
            best=ntohl(rhead[1]);
            DEBUG("Receiving: " << compLen << " bytes\n");
            // Cygwin does not know MSG_WAITALL and splits large blobs :(
            char * ptr = outBuf;
            int rest = compLen;
            while(rest>0) {
                l=recv(con, ptr, rest, MSG_WAITALL | MSG_NOSIGNAL);
                if(l<1) return false;
                ptr+=l;
                rest-=l;
            }
            DEBUG("### Received\n");
            return true;
        }

        bool doLocalCompression(int method=0) {
            const int maxalg=11;
            int z_error;

            if(method >= 0)
            {
                compLen=maxlen;
                best=method;
                z_error=compress2((Bytef*) outBuf, (uLongf*) & compLen, (Bytef*)inBuf, blocksize, method);
                if(z_error != Z_OK)
                {
                    cerr << "**** Error " << z_error << " compressing block" << endl;
                    return false;
                }
            }
            else if(method==-1) {
                compLen=maxlen;
                best=10;
                unsigned int tmp=compLen; // stupid, but needed on 64bit...
                if(!compress_zlib(shrink_extreme, (unsigned char *) outBuf, tmp, (unsigned char *)inBuf, blocksize))
                {
                    fprintf(stderr, "*** Error compressing block with 7ZIP!\n");
                    return false;
                }
                compLen=tmp;
            }
            else if(method<-1)
            {
             
                compLen=maxlen+1; // first one should win in the beginning

                char *tmpBuf=(char *) malloc(maxlen);
                unsigned long tmpLen=maxlen;

                if(!tmpBuf)
                    die("Out of Memory.");

                //int j=(method<-2)?9:0;
                for(int j=0; j<maxalg; j++) {
                    tmpLen = maxlen;
                    // DEBUG
		    // fprintf(stderr, " trying: %2d\r", j); fflush(stderr);

                    if(j<10) {
                        if((z_error=compress2((Bytef*)tmpBuf, (uLongf*)&tmpLen, (Bytef*)inBuf, blocksize, j)) != Z_OK)
                        {
                            fprintf(stderr, "*** Error %d compressing block, algo: %d!\n", z_error, j);
                            return false;
                        }
                    }
                    else { // try 7zip as the last one

                        unsigned int tmp=tmpLen; // stupid, but needed on 64bit...
                        if(!compress_zlib(shrink_extreme, (unsigned char *) tmpBuf, tmp, (unsigned char *)inBuf, blocksize))
                        {
                            fprintf(stderr, "*** Error %d compressing block with 7ZIP!\n", j);
                            return false;
                        }
                        tmpLen=tmp;
                    }

                    if(tmpLen<compLen) { // a new winner found, swap tmpBuf and compLen
                        best=j;
                        compLen=tmpLen;
                        char *t = tmpBuf;
                        tmpBuf=outBuf;
                        outBuf=t;
                    }
                }
                free(tmpBuf);
            }

            DEBUG("done");
            return true;
        }
};


void *compressingLoop(void *ptr)
{
    int id = * ( (int*) ptr);
    DEBUG("Worker Nr. " << id << " created");

    char *peer;

    int con=-1;
    if(hostpool.size()) {
        peer = hostpool[id % hostpool.size()];
        if(strcmp(peer, "LOCAL")) {
            con=setup_connection(peer);
            if(con<0)
                cerr << "Unable to connect, compressing locally\n";
        }
        // otherwise compress locally
    }

    int pos(0);
    while(!terminateAll)
    {
#if 1
        DEBUG("c1");
        lock;
get_fresh_stuff:
        DEBUG("c2");
        int i=1;
        for(;i<=poolsize;i++) {
            int j=(pos+i)%poolsize;
            if(pool[j].state==SFRESH) { // MINE!
                pool[j].state=SRESERVED;
                pos=j;
                break;
            }
        }
        DEBUG("c3, i: "<<i);
        if(i>poolsize) { doSleep; goto get_fresh_stuff; }
        DEBUG("c4");
        unlock;

#else
        // simplier/stupid version, just pickup the first one
        lock;
get_fresh_stuff:
        for(pos=0;pos<poolsize;pos++) {
            if(pool[pos].state==SFRESH) { // MINE!
                pool[pos].state=SRESERVED;
                break;
            }
        }
        if(pos==poolsize) { doSleep; goto get_fresh_stuff; }
        unlock;
#endif

do_local:
        if(con<0) {
            DEBUG("c5");
            if (! pool[pos].doLocalCompression(method) )
                die("Compression failed on block " <<pos);
        }
        else {
            DEBUG("c6");
            if(! pool[pos].doRemoteCompression(method, con) ) 
            {
                con=-1;
                cerr << "Remote compression failed, doing local now...\n";
                goto do_local;
            }
        }
        DEBUG("Calc: submitting results of pos: " << pos);
        DEBUG("c7");
        lock;
        pool[pos].state=SCOMPRESSED;
        doAwake;
        unlock;
        DEBUG("c8");
    }
    return(NULL); // g++ shut up
}

void *outputFetch(void *ptr) {

    //int id = * ( (int*) ptr);

    DEBUG("Fetcher thread created");
    uint64_t total_compressed(0);
    for(int i=0; i<maxalg; i++) levelcount[i]=0; // or better with memset?
    time_t starttime=time(NULL);
    DEBUG("f1");

    while(true) {

        int pos=posFetch%poolsize;
        DEBUG("f2");

        lock;
        DEBUG("f3");
        while(/*posFetch<=posAdd || */pool[pos].state!=SCOMPRESSED) {
            DEBUG("f4, pos: "<<pos);
            if(pool[pos].state==STOPMARK) // ugly, exiting program with hot locks... don't care
                return(NULL);
            DEBUG("f4.1");
            doSleep;
            DEBUG("f4.2");
        }
        unlock;
        DEBUG("f5");

        total_compressed += pool[pos].compLen;

        ++levelcount[pool[pos].best];

        lengths.push_back(pool[pos].compLen); // could seek, but that may be faster after all
        DEBUG("f6, target: " << targetkind);
        if(targetkind<TOMEM) 
        {
           DEBUG("f6.5");
           if(pool[pos].compLen != fwrite(pool[pos].outBuf, sizeof(char), pool[pos].compLen, datafh))
              die("Writting output");
        }
        else { //TOMEM
            char *t=(char *) malloc(pool[pos].compLen);
            if(!t) {
                cerr << "Virtual memory exhausted. Use temp. file mode or add more swap." <<endl;
                exit(1);
            }
            memcpy(t, pool[pos].outBuf, pool[pos].compLen);
            blocks.push_back(t);
        }
        DEBUG("f7");

        /* Print status  */
        if(be_verbose || 0==posFetch%100 || posFetch==(int)expected_blocks-1) {
            unsigned int per=1+time(NULL)-starttime;
            fprintf(stderr,
                    "[%2d] Blk# %5d, [ratio/avg. %3d%%/%3d%%], avg.speed: %d b/s, ETA: %ds\n",
                    pool[pos].best,
                    posFetch,
                    (int)(((float)pool[pos].compLen*(float)100) / (float)blocksize ),
                    (int)(((float) total_compressed*100) / (((float)posFetch+1)*(float)blocksize)),
                    ((posFetch+1)*blocksize)/per,
                    ( per*(expected_blocks-posFetch-1) ) / (posFetch+1)
                   );
#if 0
            fprintf(stderr, 
                    "[%2d] Blk# %5d, [ratio%3lu%%, avg.%3lu%%], avg.speed: %d b/s, ETA: %ds\n",
                    pool[pos].best,
                    posFetch, 
                    (pool[pos].compLen*100) / blocksize,
                    (total_compressed*100) / ((posFetch+1)*blocksize),
                    ((posFetch+1)*blocksize)/per,
                    ( per*(expected_blocks-posFetch-1) ) / (posFetch+1)
                   );
#endif
        }

        lock;
        pool[pos].state=SDIRTY;
        posFetch++;
        doAwake;
        unlock;
    }
    return(NULL);
}

void *inputFeed(void *ptr) {
    
    //int id = * ( (int*) ptr);
    
    DEBUG("Input thread created");

    compressItem *item;
    int newstate(SFRESH);
    bool finishing(false);

    while(true) {

        DEBUG("s1");

        int pos=posAdd%poolsize ;

        DEBUG("s3");
        lock;
        DEBUG("s4");
        while(pool[pos].state!=SDIRTY) // overrun? wait for outputFetch to mark it dirty again
            doSleep;
        DEBUG("s5");
        // not changing posAdd yet, keep it dirty
        unlock;

        DEBUG("Next block...");
        if(finishing)
            newstate=STOPMARK;
        else {
            char *ptr=pool[pos].inBuf;
            size_t rest=blocksize;
            while(rest>0) {
                ssize_t r=read(in, ptr, rest);
                if(r<0)
                    die("Input stream error");
                ptr+=r;
                rest-=r;
                if(!r)
                    break;
            }
            DEBUG("Block rest: " << rest);

            if(rest==blocksize) { // zero read, previous block was the last one
                DEBUG("s2.3");
                newstate=STOPMARK;
            }
            else if(posAdd==expected_blocks) { // got data but this block is already one too much, ignore it and bail out
                DEBUG("s2.2");
                ret=1;
                cerr << "WARNING: got more data than expected. Trailing data is ignored, your image may be incomplete." <<endl;
                newstate=STOPMARK;
            }

            if(rest>0) // padding with zeroes and making sure that the endmark will be set in the next block
            {
                memset(ptr, 0, rest);
                finishing=true;
            }
        }

        lock;
        DEBUG("Set new state on " << posAdd << ", " << newstate);
        pool[pos].state=newstate;
        posAdd++;
        doAwake; // go compressors, go
        unlock;
        if(newstate==STOPMARK) {
            DEBUG("Set stop mark on " << posAdd-1);
            return(NULL);
        }
    }
    return(NULL); 
}

int create_compressed_blocks_mt() {
    int threadId=0;
    pthread_t output_thread;

#if 0
    if(hostpool.size()) {
        int n=0;
        for(int sid=0; sid < workThreads ; sid++) {
try_another_connection:
            char *peer = hostpool[n++ % hostpool.size()];
            int con=-1;
            con=setup_connection(peer);
            if(con<0) {
                cerr << "Connection to " <<peer <<" failed. Wrong port?"<<endl;
                if(n>sid+5) die("Too many connection failures on ");
                goto try_another_connection;
            }
            DEBUG("got con: " << con << " for " <<sid);
            conpool.push_back(con);
        }
    }
#endif

    pool = new compressItem[workThreads+3];

    for(; threadId < workThreads ; threadId++)
        pthread_create(new pthread_t, NULL, compressingLoop, (void *) new int(threadId));

    pthread_create(new pthread_t, NULL, inputFeed, (void *) new int(threadId++));

    pthread_create(&output_thread, NULL, outputFetch, (void *) new int(threadId));

    int ret;
    pthread_join(output_thread, (void **) &ret);
    DEBUG("or: " << ret);

    terminateAll=true;
    
    if(!be_quiet) {
        fprintf(stderr,"\nStatistics:\n");
        for(int j=0; j<maxalg-1; j++) 
            fprintf(stderr,"gzip(%d): %5d (%5.2g%%)\n", 
                    j,
                    levelcount[j],
                    100.0F*(float)levelcount[j]/(float)lengths.size());
        fprintf(stderr,"7zip: %5d (%5.2g%%)\n", 
                levelcount[10],
                100.0F*(float)levelcount[10]/(float)lengths.size());
    }

    return ret;
};

#define OPTIONS "bB:mrp:lt:hs:f:j:a:vqS:L:"
        
int usage(char *progname)
{
    cout << "Usage: advfs [options] INFILE OUTFILE [HOSTS...]" << endl;
    cout << "Options:" << endl;
    cout << "  -b     Try all and choose the best compression method, see -L" << endl;
    cout << "  -B N   Set the block size to N" << endl;
    cout << "  -m     Use memory for temporary data storage (NOT recommended)" << endl;
    cout << "  -r     Reuse output file as temporary file (NOT recommended)"   << endl;
    cout << "  -p M   Set a default value for port number to M" <<endl;
    cout << "  -l     Listening mode (as remote node)" <<endl;
    cout << "  -t T   Total number of compressing threads" <<endl;
    cout << "  -s Q   Expect data with size Q from the input, see below" <<endl;
    cout << "  -f S   Temporary file S (or see -r)"<<endl;
    cout << "  -q     Don't print periodic status messages to the console" << endl;
    cout << "  -v     Verbose mode, print extra statistics" <<endl;
    cout << "  -h     Help of the program" << endl;
    cout << "  -S X   Experimental option: store volume header in file X, see manpage" <<endl;
    cout << "Performance tuning options:"<<endl;
    //cout << "  -j W   Jobsize, number W of blocks passed to each working thread per call"<<endl;
    cout << "  -a U   Job pool size (default: threadcount+3)" <<endl;
    cout << "  -L V   Compression level (-2..9); 9: zlib's best (default setting), 0: none,\n"
            "         -1: 7zip, -2: do all and keep the best one" <<endl;
    /*
     * does not make sense, 7zip is about 10 times slower than all gzip methods together
     * , -3: like -2 but trying\n"
            "         only gzip:9 and 7zip (faster and one of them mostly wins anyway)" << endl;
            */
    cout <<endl;
    cout << "To use standard input/output - can be used as INFILE/OUTFILE. However, this will\n"
        "require additional memory (or diskspace) for data size calculation and header\n"
        "update (after compression). Passing the INPUT data size with -s may help.\n";
    cout << "The size numbers can be declared with a suffix which acts as multiplier\n"
        "(K: KiB, k: KB, i: iso9660 block (2KiB); M: MiB; m: MB; G: GiB; g: GB)." <<endl;
    return(1);
}

uint64_t getsize(char *text) {
    
    if(!text) return 0;
    
    int map[] = {'k', 1000, 'K', 1024, 'm', 1000000, 'M', 1048576, 
        'g', 1000000000, 'G', 1073741824, 'i', 2048, 0 };
    char *mod = text + strlen(text)-1;
    uint64_t fac=0;
    if(*mod > 57) {
        for(int i=0;map[i];i+=2)
            if(map[i]==*mod)
                fac=map[i+1];
        if(!fac) die("Unknown factor " << mod << " or bad size " << text);
        *mod=0x0;
    }
    else fac=1;
    
    return fac*atoll(text);
}

inline bool is_pos_number(char *text) {
	for(char *p=text;*p;p++)
		if(!isdigit(*p))
			return false;
	return true;
}

int main(int argc, char **argv)
{
    struct cloop_head head;
    uint64_t bytes_so_far;
    char *tempfile(NULL), *sepheader(NULL);
    uint64_t datasize=0;
    int c;

#ifdef _SC_NPROCESSORS_ONLN
    workThreads=sysconf(_SC_NPROCESSORS_ONLN);
    cerr << workThreads << " processor core(s) detected\n";
#endif

    while (1)
    {
        int option_index = 0;

#ifdef HAVE_GETOPT_LONG
        static struct option long_options[] =
        {
            {"best", 0, 0, 'b'},
            {0, 0, 0, 0}
        };
        c = getopt_long (argc, argv, OPTIONS,
                long_options, &option_index);
#else
		c = getopt(argc, argv, OPTIONS);
#endif
        if (c == -1) 
            break;

        switch (c)
        {
            case 'B':
                blocksize=getsize(optarg);
                if(blocksize > 1<<20) {
                    blocksize = 1<<20;
                    cerr << "Block size is too big. Adjusting to " << blocksize<<endl;
                }
                if(blocksize < 512) {
                    blocksize = 512;
                    cerr << "Block size is too small. Adjusting to " << blocksize<<endl;
                }
                if(blocksize%512) {
                    blocksize = blocksize-blocksize%512;
                    cerr << "Block size not multiple of 512. Adjusting to " << blocksize << endl;
                }
                break;

            case 'm':
                targetkind=TOMEM;
                break;

            case 'b':
                method=-2;
                break;

            case 'L':
                method=getsize(optarg);
                if(method<-2 || method > 9)
                    die("Invalid compression method");
                break;

            case 'f':
                targetkind=TOTEMPFILE;
                tempfile=optarg;
                break;

            case 'v':
                be_verbose=true;
                break;

            case 'q':
                be_quiet=true;
                break;

            case 'r':
                reuse_as_tempfile=true;
                break;

            case 'S':
                sepheader=optarg;
                break;

            case 's':
                datasize=getsize(optarg);
                break;

            case 'a':
                poolsize=getsize(optarg);
                break;

            case 'j':
                jobsize=getsize(optarg);
                break;

            case 'p':
                defport=getsize(optarg);
                if(defport>65535) die("Invalid port");
                break;

            case 'l':
                start_server(defport);
		exit(0);
                break;

            case 't':
                {
                   int nMaxThreads=getsize(optarg);
                   if(nMaxThreads)
                      workThreads=nMaxThreads;
                   else
                      cerr << "Bad thread count, using default (" << workThreads << ")"<<endl;
                }
                break;

            default:
                usage(argv[0]);
                exit(1);
        }

    }

    const char *fromfile=NULL, *tofile=NULL;
    int test;

    if(optind > argc-2) {
        usage(argv[0]);
        die("\nInfile and outfile must be specified");
    }
    while(optind!=argc) {
        if( 0==hostpool.size() && is_pos_number(argv[optind]) )
        {
           int val=getsize(argv[optind]);
            cerr << "Warning, number as file string found. Assuming old command syntax and\n"
                "choosing compatible parameters (-m -B " << val <<"). See the usage info (-h)\n"
                "for better/correct parameters."<<endl;
            blocksize=val;
            targetkind=TOMEM;
            fromfile=tofile="-";
        }
        else if(!fromfile) fromfile=argv[optind];
        else if(!tofile) tofile = argv[optind];
        else hostpool.push_back(argv[optind]);
        optind++;
    }

    // initializing and normalizing parameters
    if(!blocksize)  blocksize=65536;
    if(!poolsize) poolsize=workThreads+3;
    if(tempfile && targetkind==TOMEM) die("Either -r or -m is allowed");
    if(reuse_as_tempfile && tempfile) die("outfile reuse with another tempfile does not make sense");
    if(sepheader && (reuse_as_tempfile || targetkind!=TOFILE ))
        die("Separate header file only with pure file output supported"); // writing twice? Later... or never

    if(!tofile)
        die("Unknown output file. Provide a path name or - for STDOUT");
    if(!fromfile)
        die("Unknown input file. Provide a path name or - for STDIN");

    if(strcmp(tofile, "-")) {
        truncate(tofile,0);
        targetfh=fopen(tofile, "w+");
        if(!targetfh)
            die("Opening output file for writing");
    }
    else {
        targetfh=stdout; // oh, that crap
        if(!sepheader && targetkind==TOFILE)
            die("Unrewindable output, choose the tempdata storage strategy.\nOne of: -m or -f <file> required, or -S for detached header");
    }

    if(strcmp(fromfile, "-")) {
        struct stat buf;
        if(!datasize) {
            stat(fromfile, &buf);
            datasize=buf.st_size;
        }
        if(datasize < 8000)
            die("Unknown or suspicious input data size. Use -s to specify a real value");

        in=open(fromfile, O_RDONLY | O_LARGEFILE);
    }
    else
    {
        in=fileno(stdin);
        if(!datasize) {
            if(sepheader) 
                cerr << "Storing volume header in " << sepheader << " and compressed data in " << tofile << ", don't forget to merge them in correct order.\n";
            else if(targetkind==TOFILE)
                die("\nUnknown input data size and no tempdata storage strategy has been choosen.\nOne of: -s, -m, -f or -r required");
        }
    }

    if(in<0) die("Opening input");

    expected_blocks=datasize/blocksize;
    if(datasize%blocksize) expected_blocks++;
    if(!expected_blocks) expected_blocks=std::numeric_limits<int>::max();

    datafh=targetfh; // for now

    if(tempfile) {
        tempfh=fopen(tempfile, "w+");
        if(!tempfh)
            die("Opening temporary file");

        datafh=tempfh;
    }

    // precalculate some values
    // expected values including additional pointer to store the initial offset
    bytes_so_far = sizeof(head) + sizeof(uint64_t) * (expected_blocks+1);
    if(!be_quiet) 
        cerr << "Block size "<< blocksize << ", expected number of blocks: " << expected_blocks <<endl;

    DEBUG("Expected data start position: " << bytes_so_far);

    if(sepheader)
        datafh=targetfh;
    else if(targetkind==TOFILE && !reuse_as_tempfile) 
        fseeko(targetfh, bytes_so_far, SEEK_SET);

    // GO, GO, GO
    if(create_compressed_blocks_mt()) 
        die("An error was detected while compressing, exiting...");

    close(in);
    fflush(datafh);

    // in tempdata modes choose real values rather than guessed
    int numblocks=expected_blocks;
    if(targetkind) {
        numblocks=lengths.size();
        bytes_so_far = sizeof(head) + sizeof(uint64_t) * (1+lengths.size());
    }
    else if(numblocks != lengths.size())
        die("Incorrect number of blocks detected, "<<numblocks << " vs. " << lengths.size());

    // stretch the temp/target file, shifting data to make space for the header
    if(reuse_as_tempfile) {
        cerr << "Shifting data..."<<endl;
        try {
            int clen=blocksize*16;
            int64_t opos = ftello(targetfh);
            char *buf = new char[clen];
            int64_t ipos=opos-clen;
            while(ipos != -clen) { // condition met after a read from 0 happened
                if(ipos<0) {
                    // last incomplete block, fix the offsets etc.
                    clen+=ipos;
                    ipos=0;
                }
                if(fseeko(targetfh, ipos, SEEK_SET) < 0) throw 42;
                if(fread( buf, sizeof(char), clen, targetfh) != clen) throw 42;
                fseeko(targetfh, ipos+bytes_so_far, SEEK_SET);
                if(fwrite(buf, sizeof(char), clen, targetfh) != clen) throw 42;
                ipos-=clen;
            }
        }
        catch(...) { die("Rewritting file"); };
    }

    if(sepheader) {
        fclose(targetfh);
        targetfh=fopen(sepheader, "w");
        if(!targetfh)
            die("Error writting header file, " << sepheader);
    }

    // seek back
    fseeko(targetfh, 0, SEEK_SET);

    /* Update the head... */

    memset(head.preamble, 0, sizeof(head.preamble));
    memcpy(head.preamble, CLOOP_PREAMBLE, sizeof(CLOOP_PREAMBLE));
    head.block_size = htonl(blocksize);
    head.num_blocks = htonl(numblocks);

    /* Write out head... */

    fwrite(&head, sizeof(head), 1, targetfh);

    if(!be_quiet) cerr << "Writing index for " << lengths.size() << " block(s)...\n";

    /* Write offsets, then data */

    // initial offset first
    uint64_t tmp;
    DEBUG("Initial offset: " << bytes_so_far << " at pos: " << ftello(targetfh));
    tmp = ENSURE64UINT(bytes_so_far);
    fwrite(&tmp, sizeof(tmp), 1, targetfh);

    for(int i=0;i<lengths.size();i++) {
        bytes_so_far += lengths[i];
        tmp = ENSURE64UINT(bytes_so_far);
        if(1!=fwrite(&tmp, sizeof(tmp), 1, targetfh))
           die("Unable to write to index area");
    }

    DEBUG("Writting data at pos: " << ftello(targetfh));

    if(!be_quiet) cerr << "Writing compressed data...\n";
    if(targetkind==TOMEM) {
        for(int i=0;i<blocks.size();i++) {
            DEBUG("Dumping contents of " << i);
            fwrite(blocks[i],lengths[i], 1, targetfh);
        }
    }
    else if(targetkind==TOTEMPFILE) {
        DEBUG("Copy back temp data");
        DEBUG("Writting data at pos: " << ftello(targetfh));
        fseeko(tempfh, 0, SEEK_SET);
        size_t maxlen=10*MAXLEN(blocksize);
        char *buf = new char[maxlen];
        while(!feof(tempfh)) {
            unsigned int len=fread(buf, sizeof(char), maxlen, tempfh);
            fwrite(buf, sizeof(char), len, targetfh);
        }
        unlink(tempfile);
    }
    if(targetfh) fclose(targetfh);
    return ret;
}





















// server code

#define PENDING 10     // how many pending connections queue will hold

void sigchld_handler(int s)
{
    while(wait(NULL) > 0);
}

int start_server(int port)
{
    int sockfd, new_fd;  // listen on sock_fd, new connection on new_fd
    struct sockaddr_in my_addr;    // my address information
    struct sockaddr_in their_addr; // connector's address information
    int yes=1;

    if ((sockfd = socket(AF_INET, SOCK_STREAM, 0)) == -1) {
        perror("socket");
        exit(1);
    }

    if (setsockopt(sockfd,SOL_SOCKET,SO_REUSEADDR,&yes,sizeof(int)) == -1) {
        perror("setsockopt");
        exit(1);
    }
    setsockopt(sockfd, IPPROTO_TCP, TCP_NODELAY, &yes, sizeof(yes));

    my_addr.sin_family = AF_INET;         // host byte order
    my_addr.sin_port = htons(port);     // short, network byte order
    my_addr.sin_addr.s_addr = INADDR_ANY; // automatically fill with my IP
    memset(&(my_addr.sin_zero), '\0', 8); // zero the rest of the struct

    if (bind(sockfd, (struct sockaddr *)&my_addr, sizeof(struct sockaddr))
            == -1) {
        perror("bind");
        exit(1);
    }

    if (listen(sockfd, PENDING) == -1) {
        perror("listen");
        exit(1);
    }

    struct sigaction sa;
    sa.sa_handler = sigchld_handler; // reap all dead processes
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = SA_RESTART;
    if (sigaction(SIGCHLD, &sa, NULL) == -1) {
        perror("sigaction");
        exit(1);
    }

    while(1) {  // main accept() loop
        socklen_t sin_size = sizeof(struct sockaddr_in);
        new_fd = accept(sockfd, (struct sockaddr *)&their_addr, &sin_size);
        if(-1 == new_fd) {
            perror("accept");
            continue;
        }
        cerr << "server: got connection from " << inet_ntoa(their_addr.sin_addr)<<endl;
        if (!fork()) { // this is the child process
            close(sockfd); // child doesn't need the listener
            unsigned int limit=1048576;
            uint32_t head[2];
            int l=recv(new_fd, head, sizeof(head), MSG_WAITALL);
            if(l<sizeof(head)) { // not OK
                close(new_fd);
                exit(1);
            }
            blocksize=ntohl(head[0]);
            uint32_t method=ntohl(head[1]);
            if( !head[0] || head[0]>limit) {
                cerr << "Bad blocksize\n";
                close(new_fd);
                exit(1);
            }
            cerr << "server: got parameters: blocksize: " << blocksize <<", method: " << method <<endl;

            // nodeid is irrelevant, use the node as permanent buffer
            compressItem item;
            while(true) {
                char * ptr = item.inBuf;
                int rest = blocksize;
                while(rest>0) {
                    l=recv(new_fd, ptr, rest, MSG_WAITALL | MSG_NOSIGNAL);
                    if(l<1) return false;
                    ptr+=l;
                    rest-=l;
                }
                if(item.doLocalCompression(method))
                    head[0]=htonl(item.compLen);
                else { 
                    close(new_fd);
                    exit(0);
                }
                head[1]=htonl(item.best);
                if (send(new_fd, head, sizeof(head), 0) == -1)
                    perror("Unable to return data");
                DEBUG("Sende: " << item.compLen << " bytes");
                rest=item.compLen;
                ptr=item.outBuf;
                while(rest>0) {
                    l= send(new_fd, ptr, rest, 0);
                    if (l<0) {
                        perror("Unable to return data");
                        exit(1);
                    }
                    rest-=l;
                    ptr+=l;
                }
            }
            close(new_fd);
            exit(0);
        }
        close(new_fd);  // parent doesn't need this
    }

    exit(0);
}

int setup_connection(char *hostname)
{
    int port;
    char *szPort=strchr(hostname, ':');
    if(szPort) {
        *szPort++ = 0x0;
        port=getsize(szPort);
    }
    else port=defport;
    
    struct sockaddr_in sa ;
    struct hostent *hp ;
    int s=-1;
    int32_t addr ;

    memset(&sa, 0, sizeof(sa)) ;
    if ((addr = inet_addr(hostname)) != -1) {
        /* is Internet addr in octet notation */
        memcpy(&sa.sin_addr, &addr, sizeof(addr)) ; /* set address */
        sa.sin_family = AF_INET ;
    } else {
        if ((hp = gethostbyname(hostname)) == NULL)
            return -2 ;
        hostname = strdup(hp->h_name) ;
        memcpy(&sa.sin_addr, hp->h_addr, hp->h_length) ;
        sa.sin_family = hp->h_addrtype ;
    }
    sa.sin_port = htons((u_short) port) ;
    DEBUG("s1");
    if ((s = socket(sa.sin_family, SOCK_STREAM, 0)) < 0)
        return -1 ;

    DEBUG("s2");

    int yes=1;
    setsockopt(s, IPPROTO_TCP, TCP_NODELAY, &yes, sizeof(yes));

    if (connect(s, (struct sockaddr *) &sa, sizeof(sa)) < 0) { 
        close(s) ;
        return -1 ;
    }
    DEBUG("s3:"<<s);
    // init the compression parameters
    uint32_t head[2];
    head[0] = htonl(blocksize);
    head[1] = htonl(method);
    if(send(s, head, sizeof(head), MSG_NOSIGNAL) == -1)
	    return -1;
    return s ;
}

#if 0
#error this is crap, stupid idea and does work worse than the split command
string outbase, outtemp;

inline void makename(size_t pos) {
    if(!chunksize)
        outtemp=outbase;
    char suf = 'a' + pos/chunksize;
    outtemp=outbase+"."+suf;
}

void split_fopen( char *path, char *mode) {
    if(out) {
        fclose(out);
        out=NULL;
    }
    if(chunk_size) {
        outbase=path;
        out_mode=mode;
        makename(0);
        path=outtemp.c_str();
    }
    out=fopen(path, mode);
}

void split_fclose() {
    if(out)
        fclose(out);
}

/*
 * Write out or die with a meaningful error message.
 */
int split_fwrite(void *ptr, size_t nmemb) {
    char *src = (char *) ptr;
    if(chunk_size) {
        off_t curpos = ftello(realfh);
        if( (curpos+nmemb) > chunk_size) {
            off_t lenA=chunk_size-curpos;
            if(lenA != fwrite(src, 1, lenA, realfh))
                goto write_error;
            realname[strlen(realname)-1]++;
            fflush(realfh);
            realfh=fopen(realname, out_mode);
            if(!realfh) die("Problems creating "<< realname);
            if(nmemb-lenA != fwrite(src+lenA, 1, nmemb-lenA, realfh))
                goto write_error;
        }
        else {
            if(nmemb != fwrite(ptr, 1, nmemb, realfh))
                goto write_error;
        }
    }
    else
        return fwrite(ptr, 1, nmemb, datafh);
    return nmemb;

write_error:
}
#endif
