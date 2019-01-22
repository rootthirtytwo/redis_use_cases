import redis
import helper as hp
import time
from redis import StrictRedis, WatchError


r = redis.Redis(host='localhost',port=6379, db=0)

r.flushall()

def create_events(event_array, available=None, price=None, tier="General"):

    e_set_key = hp.get_key_name("events")

    for event in event_array:

        if available != None:
            event['available:' + tier] = available

        if price != None:
            event['price:' + tier] = price

        e_key = hp.get_key_name("event", event["sku"])

        print(e_key)
        r.hmset(e_key,event)
        r.sadd(e_set_key, event['sku'])

def print_event_details(event_sku):
    e_key = hp.get_key_name("event", event_sku)
    print(r.hgetall(e_key))

def check_availability_and_purchase(customer, event_sku, qty, tier="General"):

    p = r.pipeline()

    try:

        e_key = hp.get_key_name("event", event_sku)
        r.watch(e_key)
        available = int(r.hget(e_key,"available:"+tier))
        price = float(r.hget(e_key, "price:" + tier))

        if available >= qty:
            p.hincrby(e_key,"available:"+tier, -qty)
            order_id = hp.get_order_id()
            purchase = {
                "order_id": order_id,
                "customer": customer,
                "tier": tier,
                "qty": qty,
                "cost": qty*price,
                "event_sku": event_sku,
                "ts": time.time()
            }

            so_key = hp.get_key_name("sales_order", str(order_id))
            p.hmset(so_key, purchase)
            p.execute()

            print("Purchase completed")

        else:
            print("Insufficient inventory, have {0}, requested {1}".format(available,qty))

    except WatchError:
        print("write conflict check_availability_and_purchase: {0}".format(e_key))

    finally:
        p.reset()

def test_check_and_purchase(events):

    print("\nCheck stock levels & purchase")
    create_events(events, available=10)

    print("\nRequest 5 tickets..")
    customer = "John"
    event_sku = "123-ABC-723"
    # check_availability_and_purchase(customer,event_sku,5)
    check_availability_and_purchase(customer, event_sku, 5)
    print_event_details(event_sku)

    print("\nRequest 5 tickets..")
    customer = "Kevin"
    event_sku = "123-ABC-723"
    # check_availability_and_purchase(customer, event_sku, 5)
    check_availability_and_purchase(customer, event_sku, 5)
    print_event_details(event_sku)

def check_purchase_reservation(customer, event_sku, qty, tier="General"):

    p = r.pipeline()

    try:

        e_key = hp.get_key_name("event", event_sku)

        r.watch(e_key)

        available = int(r.hget(e_key,"available:" + tier))

        if available >= qty:

            order_id = hp.get_order_id()
            ts = time.time()

            price = float(r.hget(e_key, "price:" + tier))

            p.hincrby(e_key, "available:" + tier, -qty)
            p.hincrby(e_key, "held:" + tier, qty)

            hold_key = hp.get_key_name("ticket_hold", event_sku)

            p.hsetnx(hold_key, "qty:" + order_id, qty)
            p.hsetnx(hold_key, "tier:" + order_id, tier)
            p.hsetnx(hold_key, "ts:" + order_id, ts)

            p.execute()
    except WatchError:
        print("Write conflict in the reservation {}".format(e_key))
    finally:
        p.reset()


    if creditcard_auth(customer, qty * price):
        try:
            purchase = {
                "order_id": order_id,
                "customer": customer,
                "tier": tier,
                "qty": qty,
                "cost": qty * price,
                "event_sku": event_sku,
                "ts": time.time()
            }

            r.watch(e_key)

            r.hdel(hold_key, "qty:" + order_id)
            r.hdel(hold_key, "tier:" + order_id)
            r.hdel(hold_key, "ts:" + order_id)

            p.hincrby(e_key, "held:" + tier, -qty)

            so_key = hp.get_key_name("sales_order", order_id)
            p.hmset(so_key, purchase)

            p.execute()

        except WatchError:
            print("Write conflict in the reservation {}".format(e_key))

        finally:
            p.reset()

        print("Purchase completed!")
    else:
        print("Auth failed on order {0} for customer {1} ${2}".format(order_id, customer, qty*price))
        backout_hold(event_sku, order_id)

def backout_hold(event_sku, order_id):

    p = r.pipeline()

    try:
        hold_key = hp.get_key_name("ticket_hold", event_sku)
        e_key = hp.get_key_name("event", event_sku)

        r.watch(e_key)

        qty = int(r.hget(hold_key, "qty:" + order_id))
        tier = str(r.hget(hold_key, "tier:" + order_id))

        p.hincrby(e_key, "available:" + tier, qty)
        p.hincrby(e_key, "held:" + tier, -qty)

        r.hdel(hold_key, "qty:" + order_id)
        r.hdel(hold_key, "tier:" + order_id)
        r.hdel(hold_key, "ts:" + order_id)

        p.execute()

    except WatchError:
        print("Write conflict in backout_hold: {}".format(e_key))
    finally:
        p.reset()

def creditcard_auth(customer, order_total):
    if customer.upper() == 'JOAN':
        return False
    else:
        return True

def test_reserve(events):
    print("Credit Card authorization process..")

    create_events(events,available=10)

    print("Reserving 5 tickets...") # success case
    customer = 'jamie'
    event_requested = "737-DEF-911"
    check_purchase_reservation(customer, event_requested, 5)
    print_event_details(event_requested)

    print("Reserving 5 tickets...") # fail case
    customer = 'joan'
    event_requested = "737-DEF-911"
    check_purchase_reservation(customer, event_requested, 5)
    print_event_details(event_requested)


if __name__=="__main__":

    events = hp.yaml_loader('data.yaml')["events"]

    print("Scenario 1")
    test_check_and_purchase(events)
    print("\nScenario 2", sep="\n")
    test_reserve(events)


