import urllib2

#Database settings
database_connect = "dbname='rid'"
kv1_database_connect = "dbname='kv1tmp'"


#NDOVLoket settings
ndovloket_url = "data.ndovloket.nl"
ndovloket_user = "voorwaarden"
ndovloket_password = "geaccepteerd"
auth_handler = urllib2.HTTPBasicAuthHandler()
auth_handler.add_password(realm=ndovloket_url,
                          uri=ndovloket_url,
                          user=ndovloket_user,
                          passwd=ndovloket_password)
opener = urllib2.build_opener(auth_handler)
urllib2.install_opener(opener)
