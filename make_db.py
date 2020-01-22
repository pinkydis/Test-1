import pymongo

from datetime import datetime


def create_and_fill_account():
    conn = pymongo.MongoClient('localhost', 27017)
    db = conn.test

    coll = db.account
    coll.remove()
    coll.insert_many([
    {
        "number" : "7800000000000",
        "name" : "Пользователь №1",
        "sessions" : [
            {
            "created_at" : datetime(2016, 1, 1, 1, 1, 0),
            "session_id" : "6QBnQhFGgDgC2FDfGwbgEaLbPMMBofPFVrVh9Pn2quooAcgxZc",
            "actions" : [ 
                {
                    "type" : "read",
                    "created_at" : datetime(2016, 1, 1, 2, 21, 1)
                }, 
                {
                    "type" : "read",
                    "created_at" : datetime(2016, 1, 1, 2, 22, 13)
                }, 
                {
                    "type" : "create",
                    "created_at" : datetime(2016, 1, 1, 2, 34, 59)
                }
            ]
            }
        ]
    },
    {
        "number" : "7800000000001",
        "name" : "Пользователь №2",
        "sessions" : [
            {
            "created_at" : datetime(2016, 1, 1, 1, 1, 0),
            "session_id" : "6QBnQhFGgDgC2FDfGwbgEaLbPMMBofPFVrVh9Pn2quooAcgxZc",
            "actions" : [ 
                {
                    "type" : "read",
                    "created_at" : datetime(2016, 1, 1, 2, 21, 1)
                }, 
                {
                    "type" : "read",
                    "created_at" : datetime(2016, 1, 1, 2, 22, 13)
                }, 
                {
                    "type" : "create",
                    "created_at" : datetime(2016, 1, 1, 2, 34, 59)
                }, 
                {
                    "type" : "delete",
                    "created_at" : datetime(2016, 1, 1, 3, 34, 59)
                }
            ]
            }
        ]
    },
        {
        "number" : "7800000000002",
        "name" : "Пользователь №3",
        "sessions" : [
            {
            "created_at" : datetime(2016, 1, 1, 1, 1, 0),
            "session_id" : "6QBnQhFGgDgC2FDfGwbgEaLbPMMBofPFVrVh9Pn2quooAcgxZc",
            "actions" : [ 
                {
                "type" : "read",
                "created_at" : datetime(2016, 1, 1, 2, 21, 1)
                }, 
                {
                "type" : "read",
                "created_at" : datetime(2016, 1, 1, 2, 22, 13)
                }, 
                {
                "type" : "create",
                "created_at" : datetime(2016, 1, 1, 2, 34, 59)
                }
            ]
            },
            {
            "created_at" : datetime(2016, 1, 1, 1, 1, 0),
            "session_id" : "6QBnQhFGgDgC2FDfGwbgEaLbPMMBofPFVrVh9Pn2quooAcgxZc",
            "actions" : [ 
                {
                    "type" : "update",
                    "created_at" : datetime(2016, 1, 1, 2, 21, 1)
                }, 
                {
                    "type" : "update",
                    "created_at" : datetime(2016, 1, 1, 2, 22, 13)
                }, 
                {
                    "type" : "read",
                    "created_at" : datetime(2016, 1, 1, 2, 34, 59)
                }
            ]
            }
        ]
    },
    {
        "number" : "7800000000003",
        "name" : "Пользователь №4",
        "sessions" : [
            {
            "created_at" : datetime(2016, 1, 1, 1, 1, 0),
            "session_id" : "6QBnQhFGgDgC2FDfGwbgEaLbPMMBofPFVrVh9Pn2quooAcgxZc",
            "actions" : [ 
                {
                    "type" : "read",
                    "created_at" : datetime(2016, 1, 1, 2, 21, 1)
                }, 
                {
                    "type" : "read",
                    "created_at" : datetime(2016, 1, 1, 2, 22, 13)
                }, 
                {
                    "type" : "create",
                    "created_at" : datetime(2016, 1, 1, 2, 34, 59)
                }, 
                {
                    "type" : "read",
                    "created_at" : datetime(2016, 1, 1, 2, 34, 59)
                }, 
                {
                    "type" : "read",
                    "created_at" : datetime(2016, 1, 1, 2, 34, 59)
                }, 
                {
                    "type" : "read",
                    "created_at" : datetime(2016, 1, 1, 2, 34, 59)
                }
            ]
            }
        ]
    }
    ])


def create_and_fill_accrual_payment():
    conn = pymongo.MongoClient('localhost', 27017)
    db = conn.test

    coll = db.accrual
    coll.remove()
    coll.insert_many([
        {
            "id" : 1,
            "date" : datetime(2018, 1, 1),
            "month" : 1
        },
        {
            "id" : 2,
            "date" : datetime(2018, 2, 1),
            "month" : 2
        },
        {
            "id" : 3,
            "date" : datetime(2018, 2, 12),
            "month" : 2
        },
        {
            "id" : 4,
            "date" : datetime(2018, 4, 1),
            "month" : 4
        },
        {
            "id" : 5,
            "date" : datetime(2018, 5, 1),
            "month" : 5
        }
        ])

    coll = db.payment
    coll.remove()
    coll.insert_many([
        {
            "id" : 1,
            "date" : datetime(2018, 1, 17),
            "month" : 1
        },
        {
            "id" : 2,
            "date" : datetime(2018, 2, 5),
            "month" : 2
        },
        {
            "id" : 3,
            "date" : datetime(2018, 2, 15),
            "month" : 2
        },
        {
            "id" : 4,
            "date" : datetime(2018, 4, 3),
            "month" : 4
        },
        {
            "id" : 5,
            "date" : datetime(2018, 5, 27),
            "month" : 5
        },
        {
            "id" : 6,
            "date" : datetime(2018, 6, 9),
            "month" : 6
        },
    ])


if __name__ == '__main__':
    create_and_fill_account()
    create_and_fill_accrual_payment()

