cd build
mkdir -p gcovr-html
GCOVR_COMMON_OPTION='-e ../third_party/ -e ../.*/test.* -e ../.*/examples.* -e ../.local/.* -e ../.*/bench/.*'
gcovr  -r .. --html --html-details  ${GCOVR_COMMON_OPTION} -o gcovr-html/kvs_charkey-gcovr.html
