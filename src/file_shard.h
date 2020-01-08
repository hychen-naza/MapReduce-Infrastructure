#pragma once

#include <vector>
#include <fstream>
#include <math.h>
#include "mapreduce_spec.h"

#define KB_SIZE 100 //1024

/* CS6210_TASK: Create your own data structure here, where you can hold information about file splits,
     that your master would use for its own bookkeeping and to convey the tasks to the workers for mapping */
struct FileShard {
	std::vector<std::string> filename;
	std::vector<int> start;
	std::vector<int> end;
};


/* CS6210_TASK: Create fileshards from the list of input files, map_kilobytes etc. using mr_spec you populated  */ 
// size 有点奇怪





inline bool shard_files(const MapReduceSpec& mr_spec, std::vector<FileShard>& fileShards) {
	int num_files = mr_spec.input_files.size();
	int fileSizes[num_files];
	std::string fileNames[num_files];
	int totalSize = 0;
	std::FILE *pFile;
	for (int i=0;i<num_files;++i) {
        std::string filename;
		filename = mr_spec.input_files[i];
		fileNames[i] = filename;
        //std::cout << "file name: " << fileNames[i] << "\n";
		pFile = std::fopen(filename.c_str(),"rb");
		std::fseek(pFile,0,SEEK_END);
		int size = std::ftell(pFile);
		fileSizes[i] = size;
		totalSize += size;
	}

	int cur = 0, fstart = 0, fend = 0;
	int left = mr_spec.shard_size*KB_SIZE;
	bool end = false, shard_end = true;
	
	FileShard *shard;
	char * c = (char *)malloc(1);
	//std::cout << "total Size : "<< totalSize<< "\n";
	//std::cout << "start shard : "<< num_shard<< "\n";
	while(1){
		if(cur==(num_files-1) && end==true) break;

		while(1){
			if(shard_end){
				shard = new FileShard;
				shard_end = false;
				left = mr_spec.shard_size*KB_SIZE;
			}
			 	
        	if(fileSizes[cur]>=left){
        		fend += left;
        		std::ifstream file;
        		file.open(fileNames[cur]);
        		if (file.is_open()) {
        			file.seekg(fend);
        			while(1){       				
        				file.read(c, 1);
        				if(file.eof()){
        					end = true;
        					break;
        				}
        				
        				if(*c == '\n') break;       
        				fend++;				
        			}
        		}
        		shard->filename.push_back(fileNames[cur]);
	            shard->start.push_back(fstart);
	            shard->end.push_back(fend);
	            fileShards.push_back(*shard);

	            fileSizes[cur] -= (fend-fstart);
	            fstart = fend + 1;
	            if(end==true && cur < (num_files-1)){
	            	cur ++;
	            	end = false;
	            	fstart = fend = 0;
	            }
        		shard_end = true;
	            break;
        	}
        	else{
				fend += fileSizes[cur];
	            shard->filename.push_back(fileNames[cur]);
	            shard->start.push_back(fstart);
	            shard->end.push_back(fend);
	            left -= fileSizes[cur];
	            fileSizes[cur] = 0;
	            end = true;
	            if(cur < (num_files-1)){
	            	cur ++;
	            	end = false;
	            	fstart = fend = 0;
	            }
	            else{
	            	fileShards.push_back(*shard);
        			shard_end = true;
	            	break;
	            }
        	}
		}
	}

	/*std::cout << "shard num is " << fileShards.size() << std::endl;
    for(int i=0;i<fileShards.size();++i){
    	FileShard shard = fileShards[i];
    	for(int j=0;j<shard.filename.size();++j){
            std::cout << "filename: " << shard.filename[j] << \
              "start: "<< shard.start[j] << " end: "<< shard.end[j] << "\n";
    	}
    	std::cout << std::endl;
    }*/
	return true;
}
