#pragma once

static size_t decideParallelBuildNumber();

static void parallel_build_mtdb(std::size_t thid, std::size_t start, std::size_t end, std::vector<Tuple*> *insertedList);

static void build_mtdb(std::vector<Tuple*> *insertedList);

/**
 * @brief delete InsertedList object.
 * @return void
 */
static void parallel_delete_mtdb(std::size_t thid, std::vector<Tuple*> *insertedList);

static void delete_mtdb(std::vector<Tuple*> *insertedList);

static bool isReady(const std::vector<char>& readys);

static void waitForReady(const std::vector<char>& readys);

static void invoke_leader();

void worker(const size_t thid, char& ready, const bool& start, const bool& quit, std::vector<Result>& res);
