"""catalog.py — the products the shop sells."""

PRODUCTS = {
    "p01": {"name": "Wireless Mouse", "category": "electronics", "price": 24.99},
    "p02": {"name": "Mechanical Keyboard", "category": "electronics", "price": 89.00},
    "p03": {"name": "USB-C Charger", "category": "electronics", "price": 19.50},
    "p04": {"name": "Noise Cancelling Headphones", "category": "electronics", "price": 199.00},
    "p05": {"name": "Webcam HD", "category": "electronics", "price": 49.99},
    "p06": {"name": "Running Shoes", "category": "sports", "price": 120.00},
    "p07": {"name": "Yoga Mat", "category": "sports", "price": 29.99},
    "p08": {"name": "Water Bottle", "category": "sports", "price": 14.99},
    "p09": {"name": "Tennis Racket", "category": "sports", "price": 85.00},
    "p10": {"name": "Coffee Beans", "category": "kitchen", "price": 16.00},
    "p11": {"name": "French Press", "category": "kitchen", "price": 34.50},
    "p12": {"name": "Chef Knife", "category": "kitchen", "price": 59.00},
    "p13": {"name": "Cast Iron Pan", "category": "kitchen", "price": 45.00},
    "p14": {"name": "Desk Lamp", "category": "home", "price": 39.99},
    "p15": {"name": "Throw Blanket", "category": "home", "price": 32.00},
    "p16": {"name": "Plant Pot", "category": "home", "price": 12.50},
    "p17": {"name": "Notebook Set", "category": "office", "price": 9.99},
    "p18": {"name": "Fountain Pen", "category": "office", "price": 42.00},
    "p19": {"name": "Backpack", "category": "office", "price": 64.00},
    "p20": {"name": "Monitor Stand", "category": "office", "price": 27.50},
}

CATEGORIES = sorted({p["category"] for p in PRODUCTS.values()})
