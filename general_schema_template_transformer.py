#!/usr/bin/env python3
import urllib.request
import yaml
import sys

#This program takes in as an argument a reference to a yaml file. It will then scan through the file looking for
# #particular tags, and then upon finding those tags, replacing them with text from other yaml files a given address

#This function accepts a file path to a yaml file and returns the contents of the yaml file as a dictionary
def input_from_yaml(inputfile):
    try:
        with open(inputfile) as file:
            yaml_template = yaml.load(file, Loader=yaml.FullLoader)
            return yaml_template
    except FileNotFoundError as e:
        print(e)
#This function accepts a url for a yaml file, and returns the contents of the yaml file in a python dictionary
def get_yaml_from_url(yaml_url):
    with urllib.request.urlopen(yaml_url) as urlfile:
        yaml_resource_file = yaml.load(urlfile, Loader=yaml.FullLoader)
        return yaml_resource_file
#This function accepts a python dictionary and outputs a yaml file to a given file path with the content of that library inside.
def output_to_yaml():
    yaml.safe_dump(outputyaml, sys.stdout, allow_unicode=True, default_flow_style=False, sort_keys=False)
    #with open('new-spec-api.yaml', 'w') as outfile:
    #   yaml.dump(outputyaml, outfile, sort_keys=False)
#This function takes in a strongly nested dictionary, then recursively traverses it looking for certain tags. It then replaces the tags with text from other files and then returns a new file.
def create_new_yaml(nested_dict):
    emptydict = {}
    for mykey, myvalue in nested_dict.items():
        mykeystring = str(mykey)
        #storagedict.clear()  trying to make sure this clears out in case it needs to run through again and needs to be empty
        if mykeystring.startswith('X-replace')==False:
            if type(myvalue) != dict:
                emptydict[mykey] = myvalue
            if type(myvalue) == dict:
                recursivevalue = create_new_yaml(myvalue)
                if recursivevalue != 2:
                    emptydict[mykey] = recursivevalue
                if recursivevalue ==2:
                    emptydict[mykey]=storagedict
                if recursivevalue ==3:
                    emptydict['enum'] = secondstorage.get('enum')
        if mykeystring.startswith('X-replace'):
            if mykeystring == 'X-replace-enum-list':
                yaml_url=str(myvalue.get('enum-file-ref'))
                if yaml_url.startswith('http') == False:
                    yaml_obj = input_from_yaml(yaml_url)
                if yaml_url.startswith('http'):
                    yaml_obj = get_yaml_from_url(yaml_url)
                replaced_section = []
                for key, value in yaml_obj.items():
                    replaced_section.append(key)
                # emptydict['enum'] = replaced_section
                secondstorage['enum'] = replaced_section
                return 3
            if mykeystring == 'X-replace-schema':
                yaml_url_list=(myvalue.get('schema-file-ref'))
                for thatkey, thatvalue in emptydict.items():
                    storagedict[thatkey] = thatvalue
                for thisitem in yaml_url_list:
                    yaml_url = str(thisitem)
                    if yaml_url.startswith('http') == False:
                        yaml_obj = input_from_yaml(yaml_url)
                    if yaml_url.startswith('http'):
                        yaml_obj = get_yaml_from_url(yaml_url)
                    #for tempkey, tempvalue in yaml_obj.items():
                    #    storagedict[tempkey] = tempvalue
                    internalcall = create_new_yaml(yaml_obj)
                    for tempkey, tempvalue in internalcall.items():
                        storagedict[tempkey] = tempvalue
                return 2


    return emptydict

#if len(sys.argv)>1: #Makes sure that there is at least one argument being passed to this program. If there is, the argument is assigned to a variable. input_from_yaml is then called with this variable as a parameter.
#   input_file = str(sys.argv[1])
try:
    input_file = str(sys.argv[1])
    yaml_template = input_from_yaml(input_file)
except Exception as e:
    print(e)



storagedict = {} #Instantiates a global dictionary. This provides temporary storage of dictionary elements to be used with create_new_yaml function
secondstorage = {} #Instantiates a second global dictionary. This is only used for collecting enum lists to use at earlier loops through the recursive function
outputyaml = create_new_yaml(yaml_template)
output_to_yaml()



#def demo_recurive_algorithm()
    #for item in nested_lookup.nested_lookup('X-replace-schema', yaml_template):
    #    yaml_url=str(item.get('schema-file-ref'))
    #    if yaml_url.startswith('http'):
    #        yaml_resource_file = get_yaml_from_url(yaml_url)
    #    if yaml_url.startswith('http') == False:
    #        yaml_resource_file = input_from_yaml(yaml_url)

        #for item in nested_lookup.nested_lookup('schemas', yaml_resource_file):
        #for key, value in yaml_resource_file.items():
        #    #if str(key).startswith('X-replace') or str(value).startswith('X-replace') ==False:
        #    print(key)
        #    print(value)
        #        #emptydict.append(key:value)


    #yaml_resource_file = get_yaml_from_url()
    #nested_lookup.nested_lookup('Donor',yaml_resource_file)

    #for item in nested_lookup.nested_lookup('X-replace-enum-list',yaml_template): #for every item in the dictionary yaml_template, it searches for a specfic phrase
    #    yaml_url=item.get('enum-file-ref')
    #    yaml_resource_file = get_yaml_from_url()
    #    replaced_section = [] #instantiates an empty list
    #    for key, value in yaml_resource_file.items():#fills the list with the keys from yaml_resource_file
    #        replaced_section.append(key) #for each key in the dictionary, add it to the list
    #    recursive_nested_dictionary_iterator(yaml_template, yaml_url) #calls the recursive program

