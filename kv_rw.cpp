#include <iostream>
#include <thread>
#include "BQ.hpp"
#include "tracer.h"

using namespace std;

static int THREAD_NUM ;
static int TEST_NUM;
static int TEST_TIME;
static int OPERATIONS_NUM;


static uint64_t key_demo = 12345678;

struct alignas(128) KV_OBJ{
    uint64_t key;
    MichaelScottQueue<uint64_t> value_queue;
};

KV_OBJ *kvlist;
bool * conflictlist;
bool * writelist;
uint64_t *runtimelist;

uint64_t ** opvaluelist;

atomic<int> stopMeasure(0);
uint64_t runner_count;

/*struct alignas(128) R_BUF{
    uint64_t r_buf;
};*/

R_BUF * r_bufs;

uint64_t g_value;

void concurrent_worker(int tid){
    uint64_t l_value = 0;
    int index = 0;
    Tracer t;
    t.startTime();
    while(stopMeasure.load(memory_order_relaxed) == 0){

        int x,y,z;
        for(size_t i = 0; i < TEST_NUM; i++){
            switch(rand() % 3) {
                case 0:
                    x = rand()%100;
                    Enqueue(&x);
                    A[threadnum]++;
                    break;
                case 1:
                    Dequeue();
                    A[threadnum]++;
                    break;
                case 2:

                    for(int j=0 ; j< OPERATIONS_NUM-1; j++) {
                        switch(rand()%2){
                            case 0 : z = rand()%100;
                                FutureEnqueue(&z);
                                A[threadnum]++;
                                break;
                            case 1: FutureDequeue();
                                A[threadnum]++;
                                break;
                        }
                    }
                    y= rand()%100;
                    Enqueue(&y);
                    A[threadnum]++;
                    break;
        }

        __sync_fetch_and_add(&runner_count,TEST_NUM);
        uint64_t tmptruntime = t.fetchTime();
        if(tmptruntime / 1000000 >= TEST_TIME){
            stopMeasure.store(1, memory_order_relaxed);
        }
    }
    runtimelist[tid] = t.getRunTime();
}


int main(int argc, char **argv){
    if (argc == 6) {
        THREAD_NUM = stol(argv[1]);
        TEST_TIME = stol(argv[2]);
        TEST_NUM = stol(argv[3]);
        OPERATIONS_NUM = stol(argv[4]);
    } else {
        printf("./kv_rw <thread_num>  <test_time> <test_num> <operations_num>\n");
        return 0;
    }

    cout<<"thread_num "<<THREAD_NUM<<endl<<
    "test_time "<<TEST_TIME<<endl<<
    "test_num "<<TEST_NUM<<endl<<
    "operations_num"<<OPERATIONS_NUM<<endl;


    opvaluelist = new uint64_t *[TEST_NUM];
    for(size_t i = 0;i < TEST_NUM;i++){
        opvaluelist[i] = new uint64_t(i);
    }

    runtimelist = new uint64_t[THREAD_NUM]();


    vector<thread> threads;
    for(size_t i = 0; i < THREAD_NUM; i++){
        threads.push_back(thread(concurrent_worker,i));
    }
    for(size_t i = 0; i < THREAD_NUM; i++){
        threads[i].join();
    }

    double runtime = 0 , throughput = 0;
    for(size_t i = 0 ; i < THREAD_NUM; i++)
        runtime += runtimelist[i];
    runtime /= THREAD_NUM;
    throughput = runner_count * 1.0 / runtime;
    cout<<"runtime "<<runtime / 1000000<<"s"<<endl;
    cout<<"***throughput "<<throughput<<endl<<endl;

}