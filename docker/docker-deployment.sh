#!/bin/bash

# Print a new line and the banner
echo
echo "==================== ENTITY-API ===================="

function get_dir_of_this_script() {
    # This function sets DIR to the directory in which this script itself is found.
    # Thank you https://stackoverflow.com/questions/59895/how-to-get-the-source-directory-of-a-bash-script-from-within-the-script-itself
    SCRIPT_SOURCE="${BASH_SOURCE[0]}"
    while [ -h "$SCRIPT_SOURCE" ]; do # resolve $SCRIPT_SOURCE until the file is no longer a symlink
        DIR="$( cd -P "$( dirname "$SCRIPT_SOURCE" )" >/dev/null 2>&1 && pwd )"
        SCRIPT_SOURCE="$(readlink "$SCRIPT_SOURCE")"
        [[ $SCRIPT_SOURCE != /* ]] && SCRIPT_SOURCE="$DIR/$SCRIPT_SOURCE" # if $SCRIPT_SOURCE was a relative symlink, we need to resolve it relative to the path where the symlink file was located
    done
    DIR="$( cd -P "$( dirname "$SCRIPT_SOURCE" )" >/dev/null 2>&1 && pwd )"
    echo 'DIR of script:' $DIR
}

# Generate the build version based on git branch name and short commit hash and write into BUILD file
function generate_build_version() {
    GIT_BRANCH_NAME=$(git branch | sed -n -e 's/^\* \(.*\)/\1/p')
    GIT_SHORT_COMMIT_HASH=$(git rev-parse --short HEAD)
    # Clear the old BUILD version and write the new one
    truncate -s 0 ../BUILD
    # Note: echo to file appends newline
    echo $GIT_BRANCH_NAME:$GIT_SHORT_COMMIT_HASH >> ../BUILD
    # Remmove the trailing newline character
    truncate -s -1 ../BUILD

    echo "BUILD(git branch name:short commit hash): $GIT_BRANCH_NAME:$GIT_SHORT_COMMIT_HASH"
}

# Set the version environment variable for the docker build
# Version number is from the VERSION file
# Also remove newlines and leading/trailing slashes if present in that VERSION file
function export_version() {
    export ENTITY_API_VERSION=$(tr -d "\n\r" < ../VERSION | xargs)
    echo "ENTITY_API_VERSION: $ENTITY_API_VERSION"
}


if [[ "$1" != "start" && "$1" != "stop" && "$1" != "down" ]]; then
    echo "Unknown command '$1', specify one of the following: start|stop|down"
else
    # Always show the script dir
    get_dir_of_this_script

    # Always export and show the version
    export_version
    
    # Always show the build in case branch changed or new commits
    generate_build_version

    # Print empty line
    echo

    if [ "$1" = "start" ]; then
        docker-compose -f docker-compose.yml -f docker-compose.deployment.yml -p entity-api up -d
    elif [ "$1" = "stop" ]; then
        docker-compose -f docker-compose.yml -f docker-compose.deployment.yml -p entity-api stop
    elif [ "$1" = "down" ]; then
        docker-compose -f docker-compose.yml -f docker-compose.deployment.yml -p entity-api down
    fi
fi

