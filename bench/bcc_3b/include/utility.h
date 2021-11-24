/**
 * @file utility.h
 */

#pragma once

#include <vector>

#include "simple_result.h"

 extern void check_flags();

extern void output_result(
	std::vector<simple_result> const& res_ol,
	std::vector<simple_result> const& res_bt);