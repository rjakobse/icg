
import sys
import os
import glob
import pyxb
import Customer
import CustomerSearchRequest
import CustomerSearchResponse
import CustomerFetchRequest2
import CustomerFetchResponse
import urllib
import xml.etree.ElementTree as ET

rm_url = r'http://srvhqfjapp13.iccompanys.com:10001/GenericClient/CCSRequestProcessor'

def call_RM_service(req, reqtype):
    reqtypes = {'1': 'CustomerUpdateRequest',
                '3': 'CustomerFetchRequest',
                '5': 'CustomerSearchRequest',
                }
    req.Type = reqtype
    req.Name = reqtypes[reqtype]
    
    req.Source="ONLINE_PROD"
    req.Destination="CoremaContextServer"
    req.UserID="ONLINE_USER"
    parameter = urllib.urlencode({'ccsRequest': req.toxml('utf-8')})
    response = urllib.urlopen(rm_url + "?%s" % parameter)
    return response.read()

def search_RM_Customer(email):
    req = CustomerSearchRequest.CCSMessage()

    ## Fill in defaults

    req.ReturnSegmentSize="10"
    req.ReturnSegmentNumber="1"
    req.SortOrder="NonCommercial"

    ##Build request Message
    EmailAddress = CustomerSearchRequest.EmailAddress(email)
    Email = CustomerSearchRequest.Email(EmailAddress)
    req.CustomerSearchData = CustomerSearchRequest.CustomerSearchData(Email)

    ##print call_RM_service(req, '5')
    LoyaltyID = False
    try:
        resp = call_RM_service(req, '5')
        root = ET.fromstring(resp)
        LoyaltyID =  root.findtext('.//LoyaltyID')
    except (pyxb.UnrecognizedContentError):
        pass
    return LoyaltyID

def get_loyalty_id_by_email(email):
   loyalty_id = search_RM_Customer(email)
   if loyalty_id:
      return loyalty_id
   return 'NONE'


