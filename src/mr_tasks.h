#pragma once

#include <string>
#include <vector>
#include <iostream>
#include <fstream>

/* CS6210_TASK Implement this data structureas per your implementation.
		You will need this when your worker is running the map task*/
struct BaseMapperInternal {

		/* DON'T change this function's signature */
		BaseMapperInternal();

		/* DON'T change this function's signature */
		void emit(const std::string& key, const std::string& val);

		/* NOW you can add below, data members and member functions as per the need of your implementation*/
		std::vector<std::vector<std::pair<std::string, int> > > keyval_pair;
		int num_interim_files;
};


/* CS6210_TASK Implement this function */
inline BaseMapperInternal::BaseMapperInternal() {

}


/* CS6210_TASK Implement this function */
inline void BaseMapperInternal::emit(const std::string& key, const std::string& val) {

	while(keyval_pair.size() < num_interim_files){
		std::vector<std::pair<std::string, int> > new_vec;
		keyval_pair.push_back(new_vec);
	}

	char start_character = key[0];
	int hash_id = start_character%num_interim_files;

	int ival = std::stoi(val);
	bool isfind = false;
	for(auto &entry : keyval_pair[hash_id]){
		if(key.compare(entry.first) == 0){
			entry.second += ival;
			isfind = true;
			break;
		}
	}
	if(!isfind){
		keyval_pair[hash_id].push_back(std::make_pair(key, ival));
	}
}


/*-----------------------------------------------------------------------------------------------*/


/* CS6210_TASK Implement this data structureas per your implementation.
		You will need this when your worker is running the reduce task*/
struct BaseReducerInternal {

		/* DON'T change this function's signature */
		BaseReducerInternal();

		/* DON'T change this function's signature */
		void emit(const std::string& key, const std::string& val);

		/* NOW you can add below, data members and member functions as per the need of your implementation*/
		std::vector<std::pair<std::string, std::string> > keyval_pair;

};


/* CS6210_TASK Implement this function */
inline BaseReducerInternal::BaseReducerInternal() {

}


/* CS6210_TASK Implement this function */
inline void BaseReducerInternal::emit(const std::string& key, const std::string& val) {
	//std::cout << "Dummy emit by BaseReducerInternal: " << key << ", " << val << std::endl;
	keyval_pair.push_back(std::make_pair(key, val));
}
