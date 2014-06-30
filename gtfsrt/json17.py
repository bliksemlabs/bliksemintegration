import xml.etree.cElementTree as ET

def stripschema(tag):
    return tag.split('}')[-1]

def get_elem_text(message, needle):
    ints = ['journeynumber', 'reinforcementnumber', 'reasontype', 'advicetype', 'passagesequencenumber', 'lagtime']

    elem = message.find('{http://bison.connekt.nl/tmi8/kv17/msg}'+needle)
    if elem is not None:
        if needle in ints:
            return int(elem.text)
        else:
            return elem.text
    else:
        return elem

def parseKV17(message, message_type, needles=[]):
    result = {'messagetype': message_type}

    for needle in needles:
        result[needle.replace('-', '_')] = get_elem_text(message, needle)

    return result

def fetchfrommessage(message):
    journey = {}
    journey_xml = message.find('{http://bison.connekt.nl/tmi8/kv17/msg}KV17JOURNEY')
    for needle in ['dataownercode', 'lineplanningnumber', 'operatingday', 'journeynumber', 'reinforcementnumber']:
        journey[needle] = get_elem_text(journey_xml, needle)

    for child in message.getchildren():
        parent_type = stripschema(child.tag)
        if parent_type == 'KV17MUTATEJOURNEY':
            required = ['timestamp']
            for subchild in child.getchildren():
                message_type = stripschema(subchild.tag)
                if message_type in ['RECOVER', 'ADD']:
                    journey['mutatejourney'] = parseKV17(subchild, message_type, required)
                elif message_type == 'CANCEL':
                    journey['mutatejourney'] = parseKV17(subchild, message_type, required + ['reasontype', 'subreasontype', 'reasoncontent', 'advicetype', 'subadvicetype', 'advicecontent'])

        elif parent_type == 'KV17MUTATEJOURNEYSTOP':
            required = ['userstopcode', 'passagesequencenumber']
            journey['mutatejourneystop'] = {'timestamp': get_elem_text(child, 'timestamp'), 'operations': []}
            for subchild in child.getchildren():
                message_type = stripschema(subchild.tag)
                if message_type == 'MUTATIONMESSAGE':
                    journey['mutatejourneystop']['operations'].append(parseKV17(subchild, message_type, required + ['reasontype', 'subreasontype', 'reasoncontent', 'advicetype', 'subadvicetype', 'advicecontent']))
                elif message_type == 'SHORTEN':
                    journey['mutatejourneystop']['operations'].append(parseKV17(subchild, message_type, required))
                elif message_type == 'LAG':
                    journey['mutatejourneystop']['operations'].append(parseKV17(subchild, message_type, required + ['lagtime']))
                elif message_type == 'CHANGEPASSTIMES':
                    journey['mutatejourneystop']['operations'].append(parseKV17(subchild, message_type, required + ['targetarrivaltime', 'targetdeparturetime', 'journeystoptype']))
                elif message_type == 'CHANGEDESTINATION':
                    journey['mutatejourneystop']['operations'].append(parseKV17(subchild, message_type, required + ['destinationcode', 'destinationname50', 'destinationname16', 'destinationdetail16', 'destinationdisplay16']))

    return journey

def kv17tojson(contents):
    xml = ET.fromstring(contents)
    results = []
    if xml.tag == '{http://bison.connekt.nl/tmi8/kv17/msg}VV_TM_PUSH':
        cvlinfo = xml.findall('{http://bison.connekt.nl/tmi8/kv17/msg}KV17cvlinfo')
        for dossier in cvlinfo:
            results.append(fetchfrommessage(dossier))
    
    return results
