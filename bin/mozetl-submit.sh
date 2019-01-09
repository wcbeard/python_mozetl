#!/bin/bash
#
# Bug 1385232 - Submission script for mozetl on EMR
#
# This is a common entry point to reduce the amount of boilerplate needed
# to run a pyspark job on airflow. ETL jobs should expose themselves through the
# mozetl.cli entry point. All necessary context for the job should be available
# though the environment in the following form: MOZETL_${COMMAND}_${OPTION}.
#
# MOZETL_COMMAND:           a command that runs a particular ETL job; this causes all
#                           arguments to be read through the environment
# MOZETL_GIT_PATH:          (optional) path to the repository
# MOZETL_GIT_BRANCH:        (optional) git branch to use
# MOZETL_SPARK_MASTER:      (optional) spark-submit --master (defaults to yarn)
# MOZETL_SUBMISSION_METHOD: (optional) specify either spark or python
#                                      (defaults to spark)
#
# Flags for running this script in development, always has precedence
# -d (dev mode)     use local repository, script must be run from within the git project
# -p (git path)     alternate to MOZETL_GIT_PATH
# -b (git branch)   alternate to MOZETL_GIT_BRANCH; ignored when using -d
# -m (spark master) alternate to MOZETL_SPARK_MASTER; useful for local setups
# -q (quiet)        turn off tracing
#
# Example usage:
#   bash airflow.sh -d -m localhost churn   # feature flags, local machine
#   MOZETL_COMMAND=churn bash airflow.sh    # environment variables, EMR

# bash "strict" mode
set -euo pipefail

if conda env list | grep -q py3; then
    conda env remove -yn py3
fi
conda env create -f /home/hadoop/analyses/bgbb/environment3.yml
PYSPARK_PYTHON='/mnt/anaconda2/envs/py3/bin/python'

# otherwise ignores intended pyspark
#/home/hadoop/pyinstall/:/usr/lib/spark/python/:/usr/lib/spark/jars/telemetry-spark-packages-assembly.jar
unset PYTHONPATH

# https://github.com/conda/conda/issues/3199
set +u
source activate py3
set -u

is_dev='false'
is_verbose='true'

# https://stackoverflow.com/a/21128172
while getopts 'dp:b:m:q' flag; do
    case ${flag} in
        d) is_dev='true' ;;
        p) MOZETL_GIT_PATH="${OPTARG}" ;;
        b) MOZETL_GIT_BRANCH="${OPTARG}" ;;
        m) MOZETL_SPARK_MASTER="${OPTARG}" ;;
        q) is_verbose='false' ;;
        *) error "Unexpected option ${flag}" ;;
    esac
done

shift $((OPTIND - 1))

if [[ "$is_verbose" = true ]]; then
    set -x
fi

# set script environment variables
MOZETL_ARGS=${MOZETL_COMMAND:-$@}
MOZETL_GIT_PATH=${MOZETL_GIT_PATH:-https://github.com/mozilla/python_mozetl.git}
MOZETL_GIT_BRANCH=${MOZETL_GIT_BRANCH:-master}
MOZETL_SPARK_MASTER=${MOZETL_SPARK_MASTER:-yarn}
MOZETL_SUBMISSION_METHOD=${MOZETL_SUBMISSION_METHOD:-spark}

# Jupyter is the default driver, execute with python instead
unset PYSPARK_DRIVER_PYTHON

# create a temporary directory for work
workdir=$(mktemp -d -t tmp.XXXXXXXXXX)
function cleanup {
  rm -rf "$workdir"
}
trap cleanup EXIT

# generate the driver script
cat <<EOT >> ${workdir}/runner.py
from mozetl import cli
cli.entry_point(auto_envvar_prefix="MOZETL")
EOT

# clone and build
if [[ "$is_dev" = true ]]; then
    cd $(git rev-parse --show-toplevel)
else
    cd ${workdir}
    git clone ${MOZETL_GIT_PATH} --branch ${MOZETL_GIT_BRANCH}
    cd python_mozetl
fi

pip install .
python setup.py bdist_egg

# echo $PYTHONPATH
python -c "import pyspark; from pyspark.sql.functions import pandas_udf, PandasUDFType;  print(pyspark.__version__)"

if [[ "${MOZETL_SUBMISSION_METHOD}" = "spark" ]]; then
    spark-submit --master ${MOZETL_SPARK_MASTER} \
                 --deploy-mode client \
                 --py-files dist/*.egg \
                 ${workdir}/runner.py ${MOZETL_ARGS}
else
    cd ${workdir}
    python ./runner.py ${MOZETL_ARGS}
fi
