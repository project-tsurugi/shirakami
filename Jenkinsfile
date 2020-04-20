pipeline {
    agent {
        docker {
            image 'project-tsurugi/oltp-sandbox'
            label 'docker'
            args '--cap-add SYS_PTRACE'
        }
    }
    environment {
        GITHUB_URL = 'https://github.com/project-tsurugi/shirakami'
        GITHUB_CHECKS = 'tsurugi-check'
        BUILD_PARALLEL_NUM="""${sh(
                returnStdout: true,
                script: 'grep processor /proc/cpuinfo | wc -l'
            )}"""
    }
    stages {
        stage ('Prepare env') {
            steps {
                sh '''
                    ssh-keyscan -t rsa github.com > ~/.ssh/known_hosts
                '''
            }
        }
        stage ('checkout master') {
            steps {
                checkout scm
                sh '''
                    git clean -dfx
                    git submodule sync --recursive
                    git submodule update --init --recursive
                '''
            }
        }
        stage ('Build masstree') {
            steps {
                sh '''
                    ./bootstrap.sh
                '''
            }
        }
        stage ('Build') {
            steps {
                sh '''
                    mkdir build
                    cd build
                    cmake -DCMAKE_BUILD_TYPE=Debug -DWAL=OFF -DENABLE_COVERAGE=ON -DENABLE_SANITIZER=ON ..
                    make clean
                    make all -j${BUILD_PARALLEL_NUM}
                '''
            }
        }
        stage ('Test') {
            environment {
                GTEST_OUTPUT="xml"
                ASAN_OPTIONS="detect_stack_use_after_return=true"
            }
            steps {
                sh '''
                    cd build
                    make test ARGS="--verbose --timeout 100"
                '''
            }
        }
        stage ('Doc') {
            steps {
                sh '''
                    cd build
                    make doxygen > doxygen.log 2>&1
                    zip -q -r shirakami-doxygen doxygen/html
                '''
            }
        }
        stage ('Coverage') {
            environment {
                GCOVR_COMMON_OPTION='-e ../third_party/ -e ../.*/test.* -e ../.*/examples.* -e .*/antlr.*'
                BUILD_PARALLEL_NUM=4
            }
            steps {
                sh '''
                    cd build
                    mkdir gcovr-xml gcovr-html
                    gcovr -j ${BUILD_PARALLEL_NUM} -r .. --xml ${GCOVR_COMMON_OPTION} -o gcovr-xml/shirakami-gcovr.xml
                    gcovr -j ${BUILD_PARALLEL_NUM} -r .. --html --html-details --html-title "shirakami coverage" ${GCOVR_COMMON_OPTION} -o gcovr-html/shirakami-gcovr.html
                    zip -q -r shirakami-coverage-report gcovr-html
                '''
            }
        }
    }
    post {
        always {
            xunit tools: ([GoogleTest(pattern: '**/*_gtest_result.xml', deleteOutputFiles: false, failIfNotNew: false, skipNoTestFiles: true, stopProcessingIfError: true)]), reduceLog: false
            recordIssues tool: gcc4(),
                enabledForFailure: true
            recordIssues tool: doxygen(pattern: 'build/doxygen.log')
            recordIssues tool: taskScanner(
                highTags: 'FIXME', normalTags: 'TODO',
                includePattern: '**/*.md,**/*.txt,**/*.in,**/*.cmake,**/*.cpp,**/*.cc,**/*.h,**/*.hpp',
                excludePattern: 'third_party/**'),
                enabledForFailure: true
            publishCoverage adapters: [coberturaAdapter('build/gcovr-xml/shirakami-gcovr.xml')], sourceFileResolver: sourceFiles('STORE_ALL_BUILD')
            archiveArtifacts allowEmptyArchive: true, artifacts: 'build/shirakami-coverage-report.zip, build/shirakami-doxygen.zip', onlyIfSuccessful: true
            notifySlack('kvs-dev', 'next-oltp', '7fb92c62-9c08-4c76-a64c-fac01404f4f3')
        }
    }
}
