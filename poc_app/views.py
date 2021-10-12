from django.shortcuts import render
from collections import Counter

from django.shortcuts import render
from django.views.decorators.csrf import csrf_exempt
from rest_framework import response, status, pagination
from rest_framework.decorators import api_view, authentication_classes, permission_classes
from rest_framework.authentication import TokenAuthentication
from rest_framework.permissions import IsAuthenticated
# from rest_framework.authtoken.models import Token
import datetime
from datetime import date, time, timedelta
from django.utils import timezone
from django.db.models import Q, Count, F, Sum, FloatField, IntegerField, ExpressionWrapper, DecimalField, Avg
from django.db.models.functions import Cast
from django.db.models.fields import DateField
import pandas as pd
import json
from collections import OrderedDict

from . helper import *


# Create your views here.

@api_view(['POST'])
def white_space_attribute_list(request):
    try:

        '''
            Required:
              X: all_attr
              Y: range_attr


            Payload: 
            {

            retailer:"RedMart"

            category:"Snacks"

            }

            Response:
            {

            x_attribs:[],

            y_attribs:[]

            }

            Query:

            attr = Attribute.objects.filter(category__name="Snacks")
            y_attr = attr.filter( Q(name__icontains = ">") | Q(surname__icontains = "<") | Q(surname__icontains = "-"))
            x_attr = attr.exclude(y_attr)

        '''

        if request.method == 'POST':
            if 'application/json' in request.headers.get('Content-Type'):
                try:
                    retailer_request = request.data.get("retailer", None)
                    category_request = request.data.get("category", None)
                    # client_request = request.data.get("client", None)
                    # country_request = request.data.get("country", None)


                except:
                    message = "Content-Type must be 'application/json', Example : {'retailer':'retailerName'}"
                    context = {"message": str(message)}
                    return response.Response(status=status.HTTP_406_NOT_ACCEPTABLE,
                                             data=context)
            else:
                message = "Content-Type must be 'application/json', Example : {'retailer':'retailerName'}"
                context = {"message": str(message)}
                return response.Response(status=status.HTTP_406_NOT_ACCEPTABLE,
                                         data=context)
            # Get retailer request

            # New Logic
            if category_request and retailer_request:

                df = get_main_df(category_request, retailer_request)

                # print(fetched)
                if len(df)>1:
                    x_attr = ["price_tier", "weight_tier", "product_size_tier", "amperage_tier", "Type"]

                    # y_attr = list(fetched.filter(attr_id__name__icontains="tier").values_list("attr_id__name",
                    #                                                                           flat=True).distinct())
                    y_attr =["price_tier", "weight_tier", "product_size_tier", "amperage_tier", "Type"]

                    context = {"x_attr": x_attr, "y_attr": y_attr}

                    return response.Response(status=status.HTTP_200_OK, data=context)

                else:
                    message = "Record not found for request: {}".format(request.data)
                    context = {"message": str(message)}
                    return response.Response(status=status.HTTP_204_NO_CONTENT, data=context)
            else:
                message = "Record not found for retailer or specified category: {}".format(retailer_request)
                context = {"message": str(message)}
                return response.Response(status=status.HTTP_204_NO_CONTENT, data=context)

        else:
            return response.Response(status=status.HTTP_405_METHOD_NOT_ALLOWED, data="Only Post Method available")

    except Exception as excepted_message:
        print(excepted_message)
        return response.Response(status=status.HTTP_204_NO_CONTENT, data={'message': str(excepted_message)})


@api_view(['POST'])
def white_space_report(request):
    try:
        '''
         {

        retailer:"RedMart",

        category:"Snacks"

        x_attrib:" ",

        y_attrib:" "

        }

        Response:
        {

        report_data:[], #same as market share

        pivot_data:[],

        yaxis_order:[]

        }

        '''

        if request.method == 'POST':
            if 'application/json' in request.headers.get('Content-Type'):
                try:
                    retailer_request = request.data.get("retailer", None)
                    # client_request = request.data.get("client", None)
                    # country_request = request.data.get("country", None)
                    category_request = request.data.get("category", None)
                    # start_date_request = request.data.get("start_date", None)
                    # end_date_request = request.data.get("end_date", None)
                    x_attr_request = request.data.get("x_attr", None)
                    y_attr_request = request.data.get("y_attr", None)
                except:
                    message = "Content-Type must be 'application/json', Example : {'retailer':'retailerName'}"
                    context = {"message": str(message)}
                    return response.Response(status=status.HTTP_406_NOT_ACCEPTABLE,
                                             data=context)
            else:
                message = "Content-Type must be 'application/json', Example : {'retailer':'retailerName'}"
                context = {"message": str(message)}
                return response.Response(status=status.HTTP_406_NOT_ACCEPTABLE,
                                         data=context)
            # Get retailer request

            # New Logic
            if retailer_request and category_request and  x_attr_request and y_attr_request:

                df = get_main_df(retailer=retailer_request, category=category_request)
                # print(ProductTimeSeries.objects.values_list())
                if len(df)>1:
                    context = generate_pivote_whitespace(
                        x_attr = x_attr_request,
                        y_attr = y_attr_request,
                        df=df
                    )

                    # result = res_df.to_dict(orient='records')
                    # context = {"data": result}
                    print(type(context))

                    # context = json.dumps(context)

                    return response.Response(status=status.HTTP_200_OK, data=context)

                else:
                    message = "Record not found for request: {}".format(request.data)
                    context = {"message": str(message)}
                    return response.Response(status=status.HTTP_204_NO_CONTENT, data=context)
            else:
                message = "One of key is not present {}".format(request.data)
                context = {"message": str(message)}
                return response.Response(status=status.HTTP_406_NOT_ACCEPTABLE, data=context)

        else:
            return response.Response(status=status.HTTP_405_METHOD_NOT_ALLOWED, data="Only Post Method available")

    except Exception as excepted_message:
        print(excepted_message)
        return response.Response(status=status.HTTP_204_NO_CONTENT, data={'message': str(excepted_message)})


@api_view(['POST'])
def market_share(request):
    try:

        '''

        Payload: 1 --> {"category":"", "retailer":"", "start_date":"date", "end_date":"date"}
                 2 --> {"category":"", "retailer":"", } --> last one month data


        If METHOD == POST and PAYLOAD.isvalid():
                Pass data to 'get_marketshare' func and return response cols
                 Response:1 --> all the product master cols + return response cols + count of ad for the partiular prod

        else:
                 Response:1 --> {"status": "Specific Message with error"}

        '''

        if request.method == 'POST':
            # Get retailer request
            if 'application/json' in request.headers.get('Content-Type'):
                try:
                    retailer_request = request.data.get("retailer", None)
                    category_request = request.data.get("category", None)
                    # country_request = request.data.get("country", None)
                    # client_request = request.data.get("client", None)
                    # start_date_request = request.data.get("start_date", None)
                    # end_date_request = request.data.get("end_date", None)

                except:
                    message = "Content-Type must be 'application/json'"
                    context = {"message": str(message)}
                    return response.Response(status=status.HTTP_406_NOT_ACCEPTABLE,
                                             data=context)
            else:
                message = "Content-Type must be 'application/json'"
                context = {"message": str(message)}
                return response.Response(status=status.HTTP_406_NOT_ACCEPTABLE,
                                         data=context)

            if retailer_request and category_request:

                res = get_main_df(retailer=retailer_request, category=category_request)
                context = res.to_json(orient="records")
                context = json.loads(context)
                return response.Response(status=status.HTTP_200_OK, data=context)

            else:
                message = "No record found"
                context = {"message": str(message)}
                return response.Response(status=status.HTTP_406_NOT_ACCEPTABLE,
                                         data=context)

        else:
            return response.Response(status=status.HTTP_405_METHOD_NOT_ALLOWED, data="Only Post Method available")

    except Exception as excepted_message:
        print(excepted_message)
        return response.Response(status=status.HTTP_204_NO_CONTENT, data={'message': str(excepted_message)})
