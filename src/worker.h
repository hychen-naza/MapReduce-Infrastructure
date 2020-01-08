#pragma once

#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <fstream>
#include <sstream>
#include <grpc++/grpc++.h>
#include <unistd.h>
#include <pthread.h>
#include <sys/syscall.h>
#include "masterworker.grpc.pb.h"
#include <mr_task_factory.h>
#include "mr_tasks.h"

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;
using masterworker::ShardInfo;
using masterworker::ShardComponent;
using masterworker::InterimFile;
using masterworker::InterimInfo;
using masterworker::OutputInfo;
using masterworker::WorkerService;

extern std::shared_ptr<BaseMapper> get_mapper_from_task_factory(const std::string& user_id);
extern std::shared_ptr<BaseReducer> get_reducer_from_task_factory(const std::string& user_id);

/* CS6210_TASK: Handle all the task a Worker is supposed to do.
	This is a big task for this project, will test your understanding of map reduce */
class Worker final : public WorkerService::Service { // not sure the name wait for change

	public:
		/* DON'T change the function signature of this constructor */
		Worker(std::string ip_addr_port);

		/* DON'T change this function's signature */
		bool run();

	private:
		std::string ip_addr_port;
		int reduce_id, map_id;

		/* NOW you can add below, data members and member functions as per the need of your implementation*/
		Status DoMap(ServerContext *context, const ShardInfo *request,
		     InterimInfo *reply) override {

			// test when workers failed
			/*if(ip_addr_port.compare("127.0.0.1:50051")==0 && map_id==2){
				exit(1);
			}
			if(ip_addr_port.compare("127.0.0.1:50052")==0 && map_id==3){
				exit(1);
			}
			if(ip_addr_port.compare("127.0.0.1:50053")==0 && map_id==4){
				exit(1);
			}*/

			auto mapper = get_mapper_from_task_factory("cs6210");
			mapper->impl_->num_interim_files = request->num_interim_files();
			std::string str;
			for (int i = 0; i < request->components_size(); i++) {
				const ShardComponent& component = request->components(i);
				const std::string& filename = component.filename();
				int start = component.start();
				int end = component.end();
				int size = end - start;

				std::ifstream file(filename);
				if (file.is_open()) {
					file.seekg(start);
					while(file.tellg()!=-1) {
						std::getline(file, str); 
						mapper->map(str);
						if(file.tellg() == end+1) break; 
						// '\n' is the seperator of shards, so once we get the last line, it should meet the end '\n' + 1
						// otherwise, have bug
					}
				}
			}

			std::vector<std::vector<std::pair<std::string, int> > >& vec = mapper->impl_->keyval_pair;
			for(int i=0;i<vec.size();++i){
				std::string interim_filename("./interim/mapper_" + ip_addr_port + "_"+ std::to_string(map_id) + "_" + std::to_string(i));
				std::ofstream file(interim_filename);
				std::vector<std::pair<std::string, int> >::iterator it;
				if (file.is_open()) {
					for (it = vec[i].begin(); it != vec[i].end(); it++) {
						file << (*it).first << "," << (*it).second << "\n";
					}
					file.close();
				} else {
					std::cout << "failed to open file " << interim_filename << "\n";
				}
				InterimFile *component_file = reply->add_components();
				component_file->set_filename(interim_filename);
			}
			
			reply->set_id(request->id());
			reply->set_worker_addr(ip_addr_port);
			std::cout<< "mapper " << ip_addr_port << " finish its work, shard id " << reply->id() << std::endl;
			map_id++;
			//sleep(2); // used for mannually shutdown workers

			return Status::OK;
		}

		class isEqual {
			public:
			    explicit isEqual(std::string UserVal) : User(UserVal) {}
			    bool operator() (const std::pair<std::string, std::vector<std::string>>& element) const {
			        return element.first == User;
			    }
			private:
			    const std::string User;
		};


		Status DoReduce(ServerContext *context, const InterimInfo *request, OutputInfo *reply) override {
			auto reducer = get_reducer_from_task_factory("cs6210");
			int shard_id = request->id();
			std::vector<std::pair<std::string, std::vector<std::string>> > vec;
			for (int i = 0; i < request->components_size(); i++){
				const InterimFile& component = request->components(i);
				const std::string& filename = component.filename();
				std::ifstream file(filename);
				std::string line;
				while (std::getline(file, line)) {
					std::istringstream is_line(line);
					std::string key;
					if (std::getline(is_line, key, ',')){
						std::string value;
						std::getline(is_line, value);
						//std::cout<<"key is : "<<key<<" value is : "<<value<<std::endl;
						auto it = std::find_if( vec.begin(), vec.end(), isEqual(key));
						if(it != vec.end()){
							(*it).second.push_back(value);
						}
						else{
							std::vector<std::string> vals;
							vals.push_back(value);
							vec.push_back(std::make_pair(key,vals));
						}
					}
				}
			}
			sort(vec.begin(), vec.end());
			for(auto it = vec.begin(); it != vec.end(); it++){
				reducer->reduce(it->first, it->second);
			}
			std::vector<std::pair<std::string, std::string> >& reduceVec = reducer->impl_->keyval_pair;
			std::string output_filename("./interim/reducer_" + ip_addr_port + "_" + std::to_string(reduce_id) +"_"+ std::to_string(shard_id));
			std::ofstream output_file(output_filename);
			std::vector<std::pair<std::string, std::string> >::iterator it;
			if (output_file.is_open()) {
				for (it = reduceVec.begin(); it != reduceVec.end(); it++) {
					output_file << (*it).first << " " << (*it).second << "\n";
				}
				output_file.close();
			} else {
				std::cout << "failed to open file " << output_filename << "\n";
			}
			reply->set_worker_addr(ip_addr_port);
			reply->set_filename(output_filename);
			reply->set_id(shard_id);
			std::cout<< "reducer " << ip_addr_port << " finish its work, shard id " << reply->id() << std::endl;
			reduce_id++;
			return Status::OK;
		}
};


/* CS6210_TASK: ip_addr_port is the only information you get when started.
	You can populate your other class data members here if you want */
Worker::Worker(std::string ip_addr_port) 
	: ip_addr_port(ip_addr_port)
{
	map_id = reduce_id = 0;
    std::string worker_address = ip_addr_port;
    ServerBuilder builder;
    builder.AddListeningPort(worker_address, grpc::InsecureServerCredentials());
    builder.RegisterService(this);
    std::unique_ptr<Server> server(builder.BuildAndStart());
    std::cout << "Server listening on " << worker_address << std::endl;
    server->Wait();

}



/* CS6210_TASK: Here you go. once this function is called your woker's job is to keep looking for new tasks 
	from Master, complete when given one and again keep looking for the next one.
	Note that you have the access to BaseMapper's member BaseMapperInternal impl_ and 
	BaseReduer's member BaseReducerInternal impl_ directly, 
	so you can manipulate them however you want when running map/reduce tasks*/
bool Worker::run() {
	/*  Below 5 lines are just examples of how you will call map and reduce
		Remove them once you start writing your own logic */ 
	/*HandleRpcs();
	std::cout << "worker.run(), I 'm not ready yet" <<std::endl;
	auto mapper = get_mapper_from_task_factory("cs6210");
	mapper->map("I m just a 'dummy', a \"dummy line\"");
	auto reducer = get_reducer_from_task_factory("cs6210");
	reducer->reduce("dummy", std::vector<std::string>({"1", "1"}));*/
	return true;
}
