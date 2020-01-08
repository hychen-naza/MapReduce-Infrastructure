#pragma once
#include <iostream>
#include <string>
#include <vector>
#include <queue>
#include <algorithm>
#include <thread>
#include <stdlib.h>
#include <cstdio>
#include <map>
#include <cstdlib>
#include <grpc++/grpc++.h>
#include <sys/types.h>
#include <unistd.h>
#include <dirent.h>
#include "mapreduce_spec.h"
#include "file_shard.h"
#include "masterworker.grpc.pb.h"

using grpc::Channel;
using grpc::CompletionQueue;
using grpc::ClientAsyncResponseReader;
using grpc::ClientContext;
using grpc::Status;
using masterworker::ShardInfo;
using masterworker::ShardComponent;
using masterworker::InterimFile;
using masterworker::InterimInfo;
using masterworker::OutputInfo;
using masterworker::WorkerService;





/* CS6210_TASK: Handle all the bookkeeping that Master is supposed to do.
	This is probably the biggest task for this project, will test your understanding of map reduce */
class Master {

	public:
		/* DON'T change the function signature of this constructor */
		Master(const MapReduceSpec&, const std::vector<FileShard>&);

		/* DON'T change this function's signature */
		bool run();

	private:

		enum task_status {NOT_START, IN_PROGRESS, COMPLETED}; 
		const MapReduceSpec& mr_spec;
		const std::vector<FileShard>& file_shards;
		int num_live_worker;
		bool mapping_done;
		bool reducing_done;
		std::thread response_listener_map;
		std::thread response_listener_reducer;
		std::thread worker_manager;
		std::mutex task_mutex;
		std::mutex worker_mutex;
		std::condition_variable worker_cv;
		// failure handle relative data structure
		int num_worker_failed;
		std::vector<std::string> failed_workers;
		std::map<std::string, std::string> worker_task_pair;

		std::vector<int> map_scoreboard;
		std::vector<int> map_inprogress_tasks;
		std::vector<int> reduce_scoreboard;
		std::vector<int> reduce_inprogress_tasks;
		std::vector<std::string> ready_worker;
		std::vector<std::vector<std::string> > interim_filenames;
		std::vector<std::string> output_filenames;
		std::map<std::string, std::unique_ptr<WorkerService::Stub> > addr_to_stub;
		CompletionQueue cq;

		struct AsyncClientCall {
	        InterimInfo reply;
	        ClientContext context;
	        Status status;
	        std::unique_ptr<ClientAsyncResponseReader<InterimInfo>> response_reader;
	    };
	    struct AsyncClientCall_reduce {
	        OutputInfo reply;
	        ClientContext context;
	        Status status;
	        std::unique_ptr<ClientAsyncResponseReader<OutputInfo>> response_reader;
	    };
		/* NOW you can add below, data members and member functions as per the need of your implementation*/
		void startMapper();
	    void AsyncCompleteRpcMap();
		void startReducer();
	    void AsyncCompleteRpcReduce();	    
	    void GetStatusofWoker(std::string worker);
	    bool CheckAddr(std::string worker);
	    void RenameAndClean();
	    void CheckFailedWorker();
};


/* CS6210_TASK: This is all the information your master will get from the framework.
	You can populate your other class data members here if you want */

Master::Master(const MapReduceSpec& mr_spec, const std::vector<FileShard>& file_shards)
	: mr_spec(mr_spec), file_shards(file_shards)
{
	mapping_done = reducing_done = false;
	for (auto &worker_addr : mr_spec.worker_addrs) {
		std::shared_ptr<Channel> channel =
			grpc::CreateChannel(worker_addr, grpc::InsecureChannelCredentials());
		std::unique_ptr<WorkerService::Stub> stub(WorkerService::NewStub(channel)); 
		addr_to_stub.insert(std::pair<std::string, std::unique_ptr<WorkerService::Stub> >(worker_addr, std::move(stub)));
		ready_worker.push_back(worker_addr);
	}

	for(int i=0;i<file_shards.size();++i){
		map_scoreboard.push_back(NOT_START);
	}
	for(int i=0;i<mr_spec.num_output_files;++i){
		reduce_scoreboard.push_back(NOT_START);
	}
	while(interim_filenames.size() < mr_spec.num_output_files){
		std::vector<std::string > new_vec;
		interim_filenames.push_back(new_vec);
	}
	num_live_worker = mr_spec.num_worker;
	num_worker_failed = 0;
	for(int i=0;i<mr_spec.worker_addrs.size();++i){
		worker_task_pair.insert(std::pair<std::string, std::string> (mr_spec.worker_addrs[i],"not start"));
	}
}


void Master::AsyncCompleteRpcMap()
{
	void *got_tag;
	bool ok = false;
	bool coming_from_straggler;
	while (cq.Next(&got_tag, &ok)) { 
		coming_from_straggler = false;
		AsyncClientCall* call = static_cast<AsyncClientCall*>(got_tag);
		GPR_ASSERT(ok);
		std::cout << "receive map messgae from "<<call->reply.worker_addr()<< " and shard id "<<call->reply.id() << std::endl;
		// check if the worker is failed or not; if failed, it will not return worker address 
		if(CheckAddr(call->reply.worker_addr())){
			// task handling
			std::unique_lock<std::mutex> task_lock(task_mutex);
			if(map_scoreboard[call->reply.id()] != COMPLETED){
				map_scoreboard[call->reply.id()] = COMPLETED;
				std::vector<int>::iterator itr = map_inprogress_tasks.begin();
				while (itr != map_inprogress_tasks.end()){
					if(*itr == call->reply.id()){
						map_inprogress_tasks.erase(itr);
						break;
					}
					itr++;
				}
			}
			else{
				coming_from_straggler = true;
			}
			task_lock.unlock();
			// worker handling
			std::unique_lock<std::mutex> worker_lock(worker_mutex);
			ready_worker.push_back(call->reply.worker_addr());
			worker_cv.notify_one();
			worker_lock.unlock();
			// interim file push
			if(!coming_from_straggler){
				for(int i=0;i<call->reply.components_size();++i){
					interim_filenames[i].push_back(call->reply.components(i).filename());
				}
			}
			else{
				for(int i=0;i<call->reply.components_size();++i){
					std::remove(call->reply.components(i).filename().c_str());
				}
			}
		}

		delete call;
		if(mapping_done==true && ready_worker.size()==num_live_worker) break;
	}
}

	
void Master::AsyncCompleteRpcReduce()
{
	void *got_tag;
	bool ok = false;
	bool coming_from_straggler;
	while (cq.Next(&got_tag, &ok)) { 
		coming_from_straggler = false;
		AsyncClientCall_reduce* reduce_call = static_cast<AsyncClientCall_reduce*>(got_tag);
		GPR_ASSERT(ok);
		std::cout << "receive reduce messgae from "<<reduce_call->reply.worker_addr()<< " and shard id "<<reduce_call->reply.id() << std::endl;
		if(CheckAddr(reduce_call->reply.worker_addr())){
			// task handling
			std::unique_lock<std::mutex> task_lock(task_mutex);		
			if(reduce_scoreboard[reduce_call->reply.id()] != COMPLETED){
				reduce_scoreboard[reduce_call->reply.id()] = COMPLETED;
				std::vector<int>::iterator itr = reduce_inprogress_tasks.begin();
				while (itr != reduce_inprogress_tasks.end()){
					if(*itr == reduce_call->reply.id()){
						reduce_inprogress_tasks.erase(itr);
						break;
					}
					itr++;
				}
			}
			else{
				coming_from_straggler = true;
			}
			
			task_lock.unlock();
			// worker handling
			std::unique_lock<std::mutex> worker_lock(worker_mutex);
			ready_worker.push_back(reduce_call->reply.worker_addr());
			worker_cv.notify_one();
			worker_lock.unlock();
			// final output file push
			if(!coming_from_straggler){
				output_filenames.push_back(reduce_call->reply.filename());
			}
		}
		delete reduce_call;
		if(reducing_done==true) break;
	}
}




void Master::startMapper(){
	int shard_id, backup_id;
	bool map_done;
	while(1){
		shard_id = -1;
		map_done = true;
		// pick task 
		std::unique_lock<std::mutex> task_lock(task_mutex);
		for(int i=0;i<map_scoreboard.size();++i){
			if(map_scoreboard[i]==NOT_START){
				shard_id = i;
				map_scoreboard[i] = IN_PROGRESS;
				map_done = false;
				break;
			}
			if(map_scoreboard[i]==IN_PROGRESS){		
				std::vector<int>::iterator itr = std::find(map_inprogress_tasks.begin(),map_inprogress_tasks.end(),i);
				if(itr==map_inprogress_tasks.end()){
					map_inprogress_tasks.push_back(i);
				}				
				map_done = false;
			}
		}
		// when map phase is close to the end, there will be no unstarted task but have inprogress tasks, due to worker failure or straggler
		if(shard_id == -1 && map_inprogress_tasks.size()>0){
			// randomly pick a inprogress task 
			shard_id = map_inprogress_tasks[(int)rand()%(map_inprogress_tasks.size())];			
		}
		task_lock.unlock();

		if(map_done) break; // we have finished map

		// pick worker 
		std::unique_lock<std::mutex> worker_lock(worker_mutex);
		while (ready_worker.size() == 0)
			worker_cv.wait(worker_lock, [this]{return ready_worker.size() > 0;});
		std::string worker = ready_worker.front();
		ready_worker.erase(ready_worker.begin());
		//GetStatusofWoker(worker);
		worker_lock.unlock();

		ShardInfo request;
		ShardComponent *component;
		int num_files = file_shards[shard_id].filename.size();
		request.set_id(shard_id);
		request.set_num_interim_files(mr_spec.num_output_files);
		for (int it = 0; it<num_files; it++) {
			component = request.add_components();
			component->set_filename(file_shards[shard_id].filename[it]);
			component->set_start(file_shards[shard_id].start[it]);
			component->set_end(file_shards[shard_id].end[it]);
		}
		std::unique_ptr<WorkerService::Stub>& stub_ = addr_to_stub.at(worker);
		AsyncClientCall *call = new AsyncClientCall;
		call->response_reader = stub_->AsyncDoMap(&call->context, request, &cq);
		call->response_reader->Finish(&call->reply, &call->status, (void*) call);
		worker_task_pair.at(worker) = "map" + std::to_string(shard_id);
		std::cout<<"master send mapper "<<worker<<" shard id "<< shard_id<<std::endl;
	}
	
}


void Master::startReducer(){
	int shard_id, backup_id;
	bool reduce_done;
	while(1){
		shard_id = -1;
		reduce_done = true;
		// pick task 
		std::unique_lock<std::mutex> task_lock(task_mutex);
		for(int i=0;i<reduce_scoreboard.size();++i){
			if(reduce_scoreboard[i]==NOT_START){
				shard_id = i;
				reduce_scoreboard[i] = IN_PROGRESS;
				reduce_done = false;
				break;
			}
			if(reduce_scoreboard[i]==IN_PROGRESS){		
				std::vector<int>::iterator itr = std::find(reduce_inprogress_tasks.begin(),reduce_inprogress_tasks.end(),i);
				if(itr==reduce_inprogress_tasks.end()){
					reduce_inprogress_tasks.push_back(i);
				}				
				reduce_done = false;
			}
		}
		if(shard_id == -1 && reduce_inprogress_tasks.size()>0){
			shard_id = reduce_inprogress_tasks[(int)rand()%(reduce_inprogress_tasks.size())];			
		}
		task_lock.unlock();

		if(reduce_done) break; // we have finished reduce

		// pick worker 
		std::unique_lock<std::mutex> worker_lock(worker_mutex);
		while (ready_worker.size() == 0)
			worker_cv.wait(worker_lock, [this]{return ready_worker.size() > 0;});
		std::string worker = ready_worker.front();
		ready_worker.erase(ready_worker.begin());
		//GetStatusofWoker(worker);
		worker_lock.unlock();
		InterimInfo request;
		request.set_id(shard_id);
		InterimFile *component;
		for(int i=0;i<interim_filenames[shard_id].size();++i){
			component = request.add_components();
			component->set_filename(interim_filenames[shard_id][i]);
		}
		std::unique_ptr<WorkerService::Stub>& stub_ = addr_to_stub.at(worker);
		AsyncClientCall_reduce *call = new AsyncClientCall_reduce;
		call->response_reader = stub_->AsyncDoReduce(&call->context, request, &cq);
		call->response_reader->Finish(&call->reply, &call->status, (void *) call);
		worker_task_pair.at(worker) = "reduce" + std::to_string(shard_id);
		std::cout<<"master send reducer "<<worker<<" shard id "<< shard_id<<std::endl;
	}
}

void Master::GetStatusofWoker(std::string worker){
	for(int i=0;i<ready_worker.size();++i){
		if(worker.compare(ready_worker[i])==0){
			std::cout << worker << "worker is now idle\n";
			return;
		}
	}
	for(int i=0;i<failed_workers.size();++i){
		if(worker.compare(failed_workers[i])==0){
			std::cout << worker << "worker is now failed\n";
			return;
		}
	}
	if(mapping_done == false){
		std::cout << worker << "worker is now busy and doing map work\n";
	}
	else if(reducing_done == false){
		std::cout << worker << "worker is now busy and doing reduce work\n";
	}
	else{
		std::cout << worker << "should not come here error\n";
	}
}




void Master::CheckFailedWorker(){

	int count = 0;
	std::map<std::string, std::string> pre_worker_task_pair; 
	std::vector<std::string> test_failed_workers;
	for(int i=0;i<mr_spec.worker_addrs.size();++i){
		pre_worker_task_pair.insert(std::pair<std::string, std::string> (mr_spec.worker_addrs[i],"work"));
	}

	while(1){
		if(reducing_done) break;
		if(num_worker_failed > 0 && failed_workers.size() < num_worker_failed){
			count = 0;
			test_failed_workers.clear();
			std::map<std::string, std::string>::iterator iter;
			for(iter = worker_task_pair.begin(); iter != worker_task_pair.end(); iter++) {
				if(iter->second.compare(pre_worker_task_pair.at(iter->first))==0){
					count++; 
					test_failed_workers.push_back(iter->first);
				}
    		}
    		// if only failed workers still do same tasks, count == num_worker_failed; 
    		// otherwise, count > num_worker_failed
    		if(count == num_worker_failed){
				failed_workers.assign(test_failed_workers.begin(), test_failed_workers.end());
				for(int i=0;i<failed_workers.size();++i){
					std::cout << "worker " << failed_workers[i] << " failed" << std::endl;
				}
    		}

    		// map copy
    		pre_worker_task_pair.clear();
    		pre_worker_task_pair.insert(worker_task_pair.begin(),worker_task_pair.end());
    		// usleep(100000); // sleep 100ms, used for mannually shutdown workers
    		usleep(1000); // sleep 1ms, used for automatically shutdown workers
		}
		else{
			std::this_thread::yield();
		}
	}
}

bool Master::CheckAddr(std::string worker){
	for(int i=0;i<mr_spec.worker_addrs.size();++i){
		if(worker.compare(mr_spec.worker_addrs[i])==0) return true;
	}
	printf("one worker failed but not know which failed yet\n");
	num_live_worker--;
	num_worker_failed++;
	return false;
}

void Master::RenameAndClean(){
	for(int i=0;i<output_filenames.size();++i){
		std::string new_name = "./output/FinalOutPut"+std::to_string(i);
		rename(output_filenames[i].c_str(), new_name.c_str());
	}

	std::string cleancmd = "rm ./interim/*";
	std::system(cleancmd.c_str());
}

/* CS6210_TASK: Here you go. once this function is called you will complete whole map reduce task and return true if succeeded */
bool Master::run() {

	//create interim dir to store interim files
	if(opendir("./interim")==NULL){
		std::string cmd = "mkdir interim";
		std::system(cmd.c_str());
		printf("interim dir create\n");
	}
	else{
		printf("interim dir exist\n");
	}

	worker_manager = std::thread(&Master::CheckFailedWorker, this);
	//start map
	response_listener_map = std::thread(&Master::AsyncCompleteRpcMap, this);
	startMapper();
	mapping_done = true;
	response_listener_map.join();
	std::cout<<"finish map and start reduce"<<std::endl;
	//start reduce
	response_listener_reducer = std::thread(&Master::AsyncCompleteRpcReduce, this);
	startReducer();
	reducing_done = true;
	response_listener_reducer.join();
	worker_manager.join();
	std::cout<<"finish map and reduce"<<std::endl;
	//clean interim files, rename output files and put it to output dir
	RenameAndClean();
	return true;
}
