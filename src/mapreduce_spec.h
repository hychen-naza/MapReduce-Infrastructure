#pragma once

#include <string>
#include <fstream>
#include <iostream>
#include <sstream>
#include <vector>


/* CS6210_TASK: Create your data structure here for storing spec from the config file */
struct MapReduceSpec {
	int num_worker;
	int num_output_files;
	int shard_size;
	std::string user_id;
	std::string output_dir;
	std::vector<std::string> worker_addrs;
	std::vector<std::string> input_files;
};



/* CS6210_TASK: Populate MapReduceSpec data structure with the specification from the config file */
inline bool read_mr_spec_from_config_file(const std::string& config_filename, MapReduceSpec& mr_spec) {
	std::ifstream config_file;
	config_file.open(config_filename);
	if(config_file.is_open()){
		std::string line;
		while (std::getline(config_file, line)){
			std::istringstream is_line(line);
			std::string key;
			if (std::getline(is_line, key, '=')) {
				std::string value;
				if (std::getline(is_line, value)) {
					std::cout << "key: " << key << " value: " << value << "\n";
					if (key.compare("n_workers") == 0) {
						mr_spec.num_worker = std::stoi(value);
					}
					else if (key.compare("worker_ipaddr_ports") == 0) {
						std::stringstream addrs(value);
						std::string addr;
						while (std::getline(addrs, addr, ',')) {
							mr_spec.worker_addrs.push_back(addr);
						}
					}
					else if (key.compare("input_files") == 0) {
						std::stringstream files(value);
						std::string filename;
						while (std::getline(files, filename, ',')) {
							mr_spec.input_files.push_back(filename);
						}
					}
					else if (key.compare("output_dir") == 0) {
						mr_spec.output_dir = value;
					}
					else if (key.compare("n_output_files") == 0) {
						mr_spec.num_output_files = std::stoi(value);
					}
					else if (key.compare("map_kilobytes") == 0) {
						mr_spec.shard_size = std::stoi(value);
					}
					else if (key.compare("user_id") == 0) {
						mr_spec.user_id = value;
					}
				}
			}
		}
		config_file.close();
		return true;
	}
	else{
		std::cerr << "Failed to open file." << std::endl;
        return false;
	}
}


/* CS6210_TASK: validate the specification read from the config file */
inline bool validate_mr_spec(const MapReduceSpec& mr_spec) {
	if(mr_spec.num_worker != mr_spec.worker_addrs.size()){
		printf("error in num of workers\n");
		return false;
	}
	return true;
}


/*int main(){
	MapReduceSpec mr_spec_;
	read_mr_spec_from_config_file("config.ini",mr_spec_);
}*/
