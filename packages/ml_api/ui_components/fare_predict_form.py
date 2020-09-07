from flask import Blueprint, render_template, request
from datetime import datetime
import requests
import json

forms_blueprints = Blueprint('forms', __name__, template_folder='templates')

@forms_blueprints.route('/', methods=['GET','POST'])
def index():
    company = ['Medallion Leasin', 'City Service', 'Blue Diamond']
    now = datetime.now()
    return render_template('TaxiFare.html', company=company, dt=now.strftime('%Y-%m-%d'), tm=now.strftime('%I:%M %p'))


@forms_blueprints.route('/thankyou', methods=['GET','POST'])
def thankyou():
    padd1 = request.form.get('PickupAddress1')
    pcity = request.form.get('PickupCity')
    pstate = request.form.get('PickupState')
    pzip = request.form.get('PickupZipCode')
    print(padd1,pcity,pstate,pzip)
    paddress = padd1.replace(' ', '+') + ',+' + \
               pcity.replace(' ', '+') + ',+' + \
               pstate.replace(' ', '+') + ',+' + \
               pzip.replace(' ', '+')
    url = 'https://maps.googleapis.com/maps/api/geocode/json?address=' + paddress + \
          '&key=AIzaSyAQREiE3MrWSepIa1bnxW_WEXvolq6uJg8'
    response = requests.get(url)
    jsonresponse = response.json()
    if jsonresponse['status'] == 'OK':
        pformattedaddress = jsonresponse['results'][0]['formatted_address']
        pickup_latitude = jsonresponse['results'][0]['geometry']['location']['lat']
        pickup_longitude = jsonresponse['results'][0]['geometry']['location']['lng']
    else:
        return render_template('ErrorPage.html', message=jsonresponse['message'])

    dadd1 = request.form.get('DropAddress1')
    dcity = request.form.get('DropCity')
    dstate = request.form.get('DropOffState')
    dzip = request.form.get('DropZipCode')
    daddress = dadd1.replace(' ', '+') + ',+' + \
               dcity.replace(' ', '+') + ',+' + \
               dstate.replace(' ', '+') + ',+' + \
               dzip.replace(' ', '+')
    url = 'https://maps.googleapis.com/maps/api/geocode/json?address=' + daddress + \
          '&key=AIzaSyAQREiE3MrWSepIa1bnxW_WEXvolq6uJg8'
    response = requests.get(url)
    jsonresponse = response.json()
    if jsonresponse['status'] == 'OK':
        dformattedaddress = jsonresponse['results'][0]['formatted_address']
        dropoff_latitude = jsonresponse['results'][0]['geometry']['location']['lat']
        dropoff_longitude = jsonresponse['results'][0]['geometry']['location']['lng']
    else:
        return render_template('ErrorPage.html', message=jsonresponse['message'])

    pickup_date_time = request.form.get('PickupDate') + ' ' + request.form.get('PickupTime')
    pickup_date_time1 = datetime.strptime(pickup_date_time, '%Y-%m-%d %H:%M')
    pickup_date_time1.strftime('yyy-MM-dd HH:mm:ss Z')

    company = request.form.get('Company')
    values = {'pickup_latitude': [pickup_latitude],
              'pickup_longitude': [pickup_longitude],
              'dropoff_latitude': [dropoff_latitude],
              'dropoff_longitude': [dropoff_longitude],
              'trip_start_timestamp': [str(pickup_date_time1)],
              'company': [company]}
    fare_response = requests.post('http://localhost:5000/v1/predict/regression', json=values)
    print(type(fare_response))
    fare = fare_response.json()
    print(fare)
    if float(fare['Predicted Fare']) < 2.70:
        fare_amount = round(2.70, 2)
    else:
        fare_amount = round(float(fare['Predicted Fare']), 2)

    return render_template('ThankYou.html', paddress=pformattedaddress, daddress=dformattedaddress,
                           pickdttm=pickup_date_time, company=company, predictedfare=fare_amount)
