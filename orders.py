
import sys

import ibapi.order_condition
from ibapi.order import (OrderComboLeg, Order)
from ibapi.common import *
from ibapi.tag_value import TagValue
from ibapi import order_condition
from ibapi.order_condition import *


class Orders:

    """ <summary>
    #/ A Market order is an order to buy or sell at the market bid or offer price. A market order may increase the likelihood of a fill 
    #/ and the speed of execution, but unlike the Limit order a Market order provides no price protection and may fill at a price far 
    #/ lower/higher than the current displayed bid/ask.
    #/ Products: BOND, CFD, EFP, CASH, FUND, FUT, FOP, OPT, STK, WAR
    </summary>"""
    @staticmethod
    def MarketOrder(action:str, quantity:float):
    
        #! [market]
        order = Order()
        order.action = action
        order.orderType = "MKT"
        order.totalQuantity = quantity
        #! [market]
        return order
