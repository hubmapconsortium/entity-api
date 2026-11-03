#!/bin/bash

# Print a new line and the banner
echo
echo "==================== Entity-API ===================="

function tier_check() {
  # Get the script name and extract DEPLOY_TIER
  SCRIPT_NAME=$(basename "${0}")

  # Extract deploy tier from script name (docker-*.sh pattern)
  if [[ ${SCRIPT_NAME} =~ docker-(.*)\.sh ]]; then
    DEPLOY_TIER="${BASH_REMATCH[1]}"
  else
    echo "Error: Script name doesn't match pattern 'docker-*.sh'"
    exit 1
  fi
  echo "Executing ${SCRIPT_NAME} to deploy in Docker on ${DEPLOY_TIER}"
}

# Chances are localhost development is not being done on an RHEL server with
# the environment variables set. Unset HOST_UID and HOST_GID to ensure
# docker-compose defaults (1001:1001) are used.
function export_host_ids() {
    if [ -n "${HOST_UID}" ] || [ -n "${HOST_GID}" ]; then
        echo "WARNING: HOST_UID and HOST_GID are set in your environment but will be ignored for localhost."
        echo "         Localhost development uses docker-compose.yml defaults."
    fi
    # Unset to ensure docker-compose defaults are used
    unset HOST_UID
    unset HOST_GID
}

# The `absent_or_newer` checks if the copied src at docker/some-api/src directory exists
# and if the source src directory is newer.
# If both conditions are true `absent_or_newer` writes an error message
# and causes script to exit with an error code.
function absent_or_newer() {
    if [ \( -e ${1} \) -a \( ${2} -nt ${1} \) ]; then
        echo "${1} is out of date"
        exit -1
    fi
}

function get_dir_of_this_script() {
    # This function sets DIR to the directory in which this script itself is found.
    # Thank you https://stackoverflow.com/questions/59895/how-to-get-the-source-directory-of-a-bash-script-from-within-the-script-itself
    SCRIPT_SOURCE="${BASH_SOURCE[0]}"
    while [ -h "${SCRIPT_SOURCE}" ]; do # resolve $SCRIPT_SOURCE until the file is no longer a symlink
        DIR="$( cd -P "$( dirname "${SCRIPT_SOURCE}" )" >/dev/null 2>&1 && pwd )"
        SCRIPT_SOURCE="$(readlink "${SCRIPT_SOURCE}")"
        [[ ${SCRIPT_SOURCE} != /* ]] && SCRIPT_SOURCE="${DIR}/${SCRIPT_SOURCE}" # if $SCRIPT_SOURCE was a relative symlink, we need to resolve it relative to the path where the symlink file was located
    done
    DIR="$( cd -P "$( dirname "${SCRIPT_SOURCE}" )" >/dev/null 2>&1 && pwd )"
    echo "DIR of script: ${DIR}"
}

# Generate the build version based on git branch name and short commit hash and write into BUILD file
function generate_build_version() {
    GIT_BRANCH_NAME=$(git branch | sed -n -e 's/^\* \(.*\)/\1/p')
    GIT_SHORT_COMMIT_HASH=$(git rev-parse --short HEAD)
    # Clear the old BUILD version and write the new one
    truncate -s 0 ../BUILD
    # Note: echo to file appends newline
    echo "${GIT_BRANCH_NAME}:${GIT_SHORT_COMMIT_HASH}" >> ../BUILD
    # Remove the trailing newline character
    truncate -s -1 ../BUILD
    echo "BUILD(git branch name:short commit hash): ${GIT_BRANCH_NAME}:${GIT_SHORT_COMMIT_HASH}"
}

# Set the version environment variable for the docker build
# Version number is from the VERSION file
# Also remove newlines and leading/trailing slashes if present in that VERSION file
function export_version() {
    export ENTITY_API_VERSION=$(tr -d "\n\r" < ../VERSION | xargs)
    echo "ENTITY_API_VERSION: ${ENTITY_API_VERSION}"
}

if [[ "${1}" != "check" && "${1}" != "config" && "${1}" != "build" && "${1}" != "start" && "${1}" != "stop" && "${1}" != "down" ]]; then
    echo "Unknown command '${1}', specify one of the following: check|config|build|start|stop|down"
else
    # Echo this script name and the tier expected for Docker deployment
    tier_check

    # Always show the script dir
    get_dir_of_this_script

    # Always export and show the version
    export_version

    # Unset HOST_UID/HOST_GID for localhost to use defaults
    export_host_ids

    # Always show the build in case branch changed or new commits
    generate_build_version

    # Print empty line
    echo

    if [ "${1}" = "check" ]; then
        # Bash array
        config_paths=(
            '../src/instance/app.cfg'
        )

        for pth in "${config_paths[@]}"; do
            if [ ! -e ${pth} ]; then
                echo "Missing file (relative path to DIR of script): ${pth}"
                exit -1
            fi
        done

        absent_or_newer entity-api/src ../src

        echo 'Checks complete, all good :)'
    elif [ "${1}" = "config" ]; then
        docker compose -f docker-compose.yml -f docker-compose.${DEPLOY_TIER}.yml -p entity-api config
    elif [ "${1}" = "build" ]; then
        # Delete the copied source code dir if exists
        if [ -d "entity-api/src" ]; then
            rm -rf entity-api/src
        fi

        # Copy over the src folder
        cp -r ../src entity-api/

        # Delete old VERSION and BUILD files if found
        if [ -f "entity-api/VERSION" ]; then
            rm -rf entity-api/VERSION
        fi

        if [ -f "entity-api/BUILD" ]; then
            rm -rf entity-api/BUILD
        fi

        # Copy over the VERSION and BUILD files
        cp ../VERSION entity-api
        cp ../BUILD entity-api

        docker compose -f docker-compose.yml -f docker-compose.${DEPLOY_TIER}.yml -p entity-api build --no-cache
    elif [ "${1}" = "start" ]; then
        docker compose -f docker-compose.yml -f docker-compose.${DEPLOY_TIER}.yml -p entity-api up -d
    elif [ "${1}" = "stop" ]; then
        docker compose -f docker-compose.yml -f docker-compose.${DEPLOY_TIER}.yml -p entity-api stop
    elif [ "${1}" = "down" ]; then
        docker compose -f docker-compose.yml -f docker-compose.${DEPLOY_TIER}.yml -p entity-api down
    fi
fi