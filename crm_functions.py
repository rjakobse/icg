
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
import copy
import datetime
import unicodecsv

#Error reporting
import traceback
import smtplib
from email.mime.text import MIMEText

import logging
logging.basicConfig(format='%(asctime)s %(levelname)s:%(message)s', filename='peak_join.log',level=logging.DEBUG)
import xml.etree.ElementTree as ET

rm_url = r'http://srvhqfjapp13:10001/GenericClient/CCSRequestProcessor'
#rm_url = r'http://srvhqfjapp23:10001/GenericClient/CCSRequestProcessor' # Test

def createCSSMessage(reqtype, payload, attribs = None):
    reqtypes = {'1': 'CustomerUpdateRequest',
                '3': 'CustomerFetchRequest',
                '5': 'CustomerSearchRequest',
                }
    msg = ET.Element('CCSMessage')
    msg.attrib['Type'] = reqtype
    msg.attrib['Name'] = reqtypes[reqtype]
    msg.attrib['Source'] ="ONLINE_PROD"
    msg.attrib['Destination'] ="CoremaContextServer"
    msg.attrib['UserID'] ="ONLINE_USER"
    
    if attribs is not None:
        msg.attrib.update(attribs)
        
    msg.append(payload)
    return msg

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
    #print req.toxml('utf-8')
    parameter = urllib.urlencode({'ccsRequest': req.toxml('utf-8')})
    response = urllib.urlopen(rm_url + "?%s" % parameter)
    return response.read()

def search_RM_Customer(email):
    req = CustomerSearchRequest.CCSMessage()

    ## Fill in defaults
    req.ReturnSegmentSize="10"
    req.ReturnSegmentNumber="1"
    req.SortOrder="NonCommercial"

    # Build request Message
    EmailAddress = CustomerSearchRequest.EmailAddress(email)
    Email = CustomerSearchRequest.Email(EmailAddress)
    req.CustomerSearchData = CustomerSearchRequest.CustomerSearchData(Email)

    # print call_RM_service(req, '5')
    loyaltyID = None
    try:
        resp = call_RM_service(req, '5')
        #print resp
        root = ET.fromstring(resp)
        LoyaltyID =  root.findtext('.//LoyaltyID')
        #print "Search: ",LoyaltyID, email
    except (pyxb.UnrecognizedContentError):
        #print "Search failed"
        pass
    return LoyaltyID

def update_RM_Customer(LoyaltyID,interests):
    ## Fetch Data
    cfd_el = ET.Element('CustomerFetchData')
    ly_el = ET.SubElement(cfd_el,'LoyaltyID')
    ly_el.text = LoyaltyID 
    msg = createCSSMessage("3", cfd_el, {'IncludeLoyaltySummary':"true", 'LimitLoyaltyTransactions':"1", 'LoyaltyTransactionsEndDate':"2013-12-31", 'LoyaltyTransactionsStartDate':"2011-01-01"})
    #print ET.tostring(msg)
    parameter = urllib.urlencode({'ccsRequest': ET.tostring(msg)})
    resp = urllib.urlopen(rm_url + "?%s" % parameter).read()
    status = 'EXCEPTION'
    #if 1 == 1:
    try:
       #print resp
       root = ET.fromstring(resp)
       cust_el = copy.deepcopy(root.find('Customer'))
 
       #print "XML to update", ET.tostring(cust_el)
       status_el = cust_el.find('./Status')
       ints_el = status_el.find('Interests')
       for int_el in ints_el.findall('Interest'):
          # If we do not remove the tags that we do not need, the requests fail.
          for child in int_el:
             #print "Tag: ", child.tag
             if child.tag not in ['InterestID', 'Value','InterestJoinLocation','InterestPreferredLocation']:
                #print "removing: ", child 
                int_el.remove(child)
          int_id_el   = int_el.find('InterestID')
          interest_id = int(int_id_el.text)

          if interest_id in interests: # and interests[interest_id] != 'Y': 
             if interests[interest_id][0] != 'Y':
                int_val_el  = int_el.find('Value')
                int_val_el.text = 'Y'

             if interests[interest_id][1] != interests[interest_id][3] and interests[interest_id][3] > 10000:
                join_store = interests[interest_id][3]
                join_store_el = int_el.find('InterestJoinLocation')
                if join_store_el is None:
                   join_store_el = ET.subelement(int_el,'InterestJoinLocation')
                if join_store_el.text != join_store:
                   join_store_el.text  = join_store

             if interests[interest_id][2] != interests[interest_id][4] and interests[interest_id][4] > 10000:
                pref_store = interests[interest_id][4]
                pref_store_el = int_el.find('InterestPreferredLocation')
                if pref_store_el is None:
                   pref_store_el = ET.subelement(int_el,'InterestPreferredLocation')
                if pref_store_el.text != pref_store:
                   pref_store_el.text  = pref_store
       ls_id = cust_el.find('./LoyaltySummary')
       cust_el.remove(ls_id)

       #print "XML to update", ET.tostring(cust_el)
       msg = createCSSMessage("1", cust_el, {'ReasonCode': "2", 'Comment':"Peak sync of Customer data from SF to FJ CHGXXXXX", 'RequestMode': 'Change'})

       parameter = urllib.urlencode({'ccsRequest': ET.tostring(msg)})
       #print "Update req: ", ET.tostring(msg)
       response = urllib.urlopen(rm_url + "?%s" %(parameter))
       resp = response.read()
       root = ET.fromstring(resp)
       status = root.findtext('*/Status')
       return status
    except Exception as e:
       tb = traceback.format_exc()
       print 'Exception: ',e
       print status
       print tb
#       #print child.tag, child.attrib
#         cust_rd = cust_el #.find('./Customer')
#         cust_id = cust_rd.attrib['CustomerID']
#         #cust_tm = cust_rd.attrib['TimeStamp']
#         #print cust_rd.attrib, cust_rd.text
#         cust_el.attrib['CustomerID'] = cust_id
#         #cust_el.attrib['TimeStamp'] = cust_tm
#         name_el = cust_rd.find('./Name')
#         createNodeBlock(['SingleName','Title','First','MI','Last','Suffix'], customer, name_el)
#         address_el = None
#         if customer.has_key('AddressLine1'):
#             address_el = cust_rd.find('./Address',)
#             el = address_el.find('./AddressType')
#             el.text = 'P'
#             createNodeBlock(['AddressLine1','AddressLine2','AddressLine3','City', 'StateProvinceCode','PostalCode', 'CountryCode'], customer, address_el)
#         elif customer.has_key('COUNTRY'):
#             address_el = cust_rd.find('./Address',)
#             el = address_el.find('./AddressType')
#             el.text = 'P'
#             createNodeBlock(['CountryCode'], customer, address_el)            
#         phone_el = None    
#         if customer.has_key('PhoneNumber'):
#             phone_el = cust_rd.find('./Phone')
#             createNodeBlock(['PhoneNumber'], customer, phone_el)
#             el = ET.SubElement(phone_el, 'PhoneType')
#             el.text = 'D'
#         email_el = cust_rd.find('./Email')
#         createNodeBlock(['EmailAddress'], customer, email_el)
#         el = email_el.find('./EmailType')
#         el.text = 'P'
#         
#         status_el = cust_rd.find('./Status')
#         
#         uniq_el = cust_rd.find('UniqueInfo')
#         
#         gender_el = uniq_el.find('Gender')
#         if gender_el is not None:
#             gender_el.text = customer['GENDER'][:1].upper()
#         else:
#             print "Gender not found"
#             el = ET.SubElement(uniq_el, 'Gender')
#             el.text = customer['GENDER'][:1].upper()
#              
#         ints_el = status_el.find('Interests')
#         # Clean up Interest nodes
#         for int_el in ints_el.findall('Interest'):
#             for child in int_el:
#                 #print "Tag: ", child.tag
#                 if child.tag not in ['InterestID', 'Value','InterestJoinLocation','InterestPreferredLocation']:
#                     #print "removing: ", child 
#                     int_el.remove(child)
#         #print ET.tostring(ints_el)        
#         for k,v in interests.items():
#            if k == 'peak_mail':
#               int_upd = False
#               for int_el in ints_el.findall('Interest'):
#                  iid_el = int_el.find('InterestID')
#                  if iid_el.text == str(v):
#                     val_id = int_el.find('Value')
#                     val_id.text = customer['NEWSLETTER'] if customer['NEWSLETTER'] > '' else 'Y'
#                     int_upd = True
#               if not int_upd:
#                  int_el = ET.SubElement(ints_el, 'Interest')
#                  iid_el = ET.SubElement(int_el, 'InterestID')
#                  iid_el.text=str(v)
#                  val_el = ET.SubElement(int_el, 'Value')
#                  val_el.text= customer['NEWSLETTER'] if customer['NEWSLETTER'] > '' else 'Y'
#            int_upd = False
#            if k == 'peak_reg':
#               ## If the value already exists in the XML, then do nothing.
#               emv_join_store = customer['JOIN_STORE_PR_BRAND'] if 'JOIN_STORE_PR_BRAND' in customer.keys() else ''
#               emv_pref_store = customer['PREFSTORE']           if 'PREFSTORE'           in customer.keys() else ''
#               if emv_join_store > '':
#                  # Loop to find the interest in question.
#                  for int_el in ints_el.findall('Interest'):
#                     iid_el = int_el.find('InterestID')
#                     if iid_el.text == str(v):
# 
#                        int_upd = True
#                        ijl_el = int_el.find('InterestJoinLocation')
#                        if ijl_el is None:
#                           ijl_el = ET.SubElement(int_el,'InterestJoinLocation')
#                        if ijl_el.text <= '':
#                           ijl_el.text = emv_join_store
# 
#                        ipl_el = int_el.find('InterestPreferredLocation')
#                        if ipl_el is None:
#                           ipl_el = ET.SubElement(int_el,'InterestPreferredLocation')
#                        if emv_pref_store > '':
#                           ipl_el.text = emv_pref_store
#                  if not int_upd:
#                     int_el = ET.SubElement(ints_el, 'Interest')
#                     iid_el = ET.SubElement(int_el , 'InterestID')
#                     iid_el.text=str(v)
#                     ijl_el = ET.SubElement(int_el,'InterestJoinLocation')
#                     ijl_el.text = emv_join_store
#                     ipl_el = ET.SubElement(int_el,'InterestPreferredLocation')
#                     ipl_el.text = emv_pref_store
# 
#         ls_id = cust_el.find('./LoyaltySummary')
#         cust_el.remove(ls_id)
#         
#        if status == 'FAILURE':
#            err_desc = root.findtext('*/*/Description')
#            if err_desc:
#                print err_desc
#                status = status + ': ' + err_desc
#            #print resp
#            print status
#
