#pragma once

static bool isReady(const std::vector<char>& readys); // NOLINT

static void waitForReady(const std::vector<char>& readys);

static void invoke_leader();

void worker(size_t thid, char& ready, const bool& start, const bool& quit,
            std::vector<Result>& res);
