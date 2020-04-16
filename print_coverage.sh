cd build
mkdir -p gcovr-html
GCOVR_COMMON_OPTION='-e ../third_party/ -e ../.*/test.* -e ../.*/examples.* -e ../.local/.* -e ../.*/bench/.*'
gcovr -s -r .. ${GCOVR_COMMON_OPTION}
