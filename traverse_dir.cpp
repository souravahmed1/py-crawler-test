#include <iostream>
#include <boost/filesystem.hpp>
#include <chrono>

#include <sys/stat.h>
#include <cstring>



namespace fs = boost::filesystem;
static long int fileCounter = 0;
static long int folderCounter = 0;

void getFileStatus(const fs::path& filePath) {
    struct stat fileInfo;
    if (stat(filePath.string().c_str(), &fileInfo) != 0) {
        std::cerr << "Failed to get file status" << std::endl;
        return;
    }
    std::cout << "File path: " << filePath << std::endl;
    std::cout << "File size: " << fileInfo.st_size << " bytes" << std::endl;
    std::cout << "Last modified: " << std::ctime(&fileInfo.st_mtime);
    // Add more metadata as needed
}

void traverseDirectory(fs::path directory) {
    if (fs::is_directory(directory)) {
        for (fs::directory_entry& entry : fs::directory_iterator(directory)) {
            if (fs::is_directory(entry)) {
                // std::cout << "Found subdirectory: " << entry.path() << std::endl;
                folderCounter++;
                traverseDirectory(entry.path()); // Only traverse subdirectories
            }
            else if (fs::is_regular_file(entry)) {
                // std::cout << "File path: " << entry.path() << std::endl;
                // std::cout << "File size: " << fs::file_size(entry) << " bytes" << std::endl;
                // std::cout << "Last modified: " << fs::last_write_time(entry) << std::endl;
                // // Add more metadata as needed
                
                json = getFileStatus(entry.path());
                fileCounter++;
            }
        }
    }
}

int main() {
    // Get the current time
    auto start_time = std::chrono::high_resolution_clock::now();

    fs::path directoryPath = "/Users/inno/Documents";
    traverseDirectory(directoryPath);

    auto end_time = std::chrono::high_resolution_clock::now();
     auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time).count();

    // Output the duration in seconds and microseconds
    std::cout << "Execution time: " << duration / 1000000 << " seconds, " << duration % 1000000 << " microseconds" << std::endl;
    std::cout << "Total Files: " << fileCounter << std::endl;
    std::cout << "Total folderCounter: " << folderCounter << std::endl;

    return 0;
}


// g++ -std=c++11 traverse_dir.cpp -o traverse_dir -lboost_filesystem -lboost_system



