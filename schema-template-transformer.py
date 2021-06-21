import nested_lookup
import urllib.request
import yaml

#This python program takes in a template yaml file. It then reads through the file looking for
#specific tags. When it reaches those tags, it begins looking for a url. It then fetches another
#yaml file from that url to extract information from it. That info is used to create a new yaml file.
#The purpose of this script is to quickly replace a portion of a yaml file with another that is located
#somewhere else.

tag='X-replace-enum-list' #this is the value that is searched for within the dictionary. For now it is hard coded, but could be passed as argument
with open('entity-api-spec-TEMPLATE.yaml') as file: #opens yaml file and sets it to 'file'
    yaml_template = yaml.load(file, Loader=yaml.FullLoader)  #creates a dictionary called yaml_template and loads the yaml file 'file' to it.

    for item in nested_lookup.nested_lookup('X-replace-enum-list',yaml_template): #for every item in the dictionary yaml_template, it searches for a specfic phrase
        tag_value_string = str(item) #when the given phrase is found, the corresponding value to that key is placed as a string inside tag_string
        url_start = tag_value_string.find('https') #marks the start point of the string as the beginning of the url. Will want to create a cleaner implementation later
        url_end = tag_value_string.find('\'}') #marks the end of the string as just before the '}
        tag_value_string = tag_value_string[url_start:url_end] #modifies the string to include only the url.
        yaml_url = tag_value_string #renaming yaml_url to be more descriptive

        with urllib.request.urlopen(yaml_url) as urlfile: #opens a yaml file from a url and sets it to 'urlfile'
            yaml_resource_file = yaml.load(urlfile, Loader=yaml.FullLoader) #creates a dictionary called yaml_resource file that holds the contents of the yaml file
            replaced_section = [] #instantiates an empty list

            for key, value in yaml_resource_file.items():#fills the list with the keys from yaml_resource_file
                replaced_section.append(key) #for each key in the dictionary, add it to the list

            def recursive_nested_dictionary_iterator(nested_dictionary): #recursive function that iterates through the template yaml file new_yaml_template
                for thiskey, thisvalue in nested_dictionary.items():
                    depth = 1 #variable that counts iterations through the recursive function.
                    thisvaluestring= str(thisvalue) #creates a string of the current value of a given key for comparing to other strings
                    if thisvaluestring == tag_value_string: #if the value matches our tag, we replace it with our new list
                        return 2 #return to the previous iteration. The value we look for to determine where we're replacing data is 3 layers deeper than the data itself.
                    if type(thisvalue) is dict: #so we need a way to go backwards 3 steps in our recursive function once we find that location.
                        depth = recursive_nested_dictionary_iterator(thisvalue) #else it continues recursively through the list
                    if depth == 2:
                        return 3 #returns to previous iteration
                    if depth == 3:
                        return 4
                    if depth == 4: #we are now at the correct location we want to change
                        thisvalue.update({'enum': replaced_section}) #update the value with the list we created earlier. The key is hard coded for now

            recursive_nested_dictionary_iterator(yaml_template) #calls the recursive program

with open('new_entity_spec_api.yaml', 'w') as outfile:
    yamloutput = yaml.dump(yaml_template, outfile, sort_keys=False, )