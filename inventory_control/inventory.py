import redis
import helper as hp
import time

r = redis.Redis(host='localhost',port=6379, db=0)
r.flushall()

def create_events(event_array, available=None, price=None, tier="General"):

    e_set_key = hp.get_key_name("events")
    print(e_set_key)
    for event in event_array:

        if available != None:
            event['available:' + tier] = available

        if price != None:
            event['price:' + tier] = price

        e_key = hp.get_key_name("event", event["sku"])

        print(e_key)
        r.hmset(e_key,event)
        r.sadd(e_set_key, event['sku'])


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

    except:
        print("write conflict check_availability_and_purchase: {0}".format(e_key))

    finally:
        p.reset()



def print_event_details(event_sku):
    e_key = hp.get_key_name("event", event_sku)
    print(r.hgetall(e_key))

def test_check_and_purchase(events):

    print("\nCheck stock levels & purchase")
    create_events(events, available=10)

    print("\nRequest 5 tickets..")
    customer = "John"
    event_sku = "123-ABC-723"
    check_availability_and_purchase(customer,event_sku,5)
    print_event_details(event_sku)

    print("\nRequest 5 tickets..")
    customer = "Kevin"
    event_sku = "123-ABC-723"
    check_availability_and_purchase(customer, event_sku, 5)
    print_event_details(event_sku)



if __name__=="__main__":

    events = hp.yaml_loader('data.yaml')["events"]

    test_check_and_purchase(events)
    # print(hp.get_key_name("events"))
