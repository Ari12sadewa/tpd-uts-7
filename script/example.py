import pandas as pd
# import uuid

df = pd.read_csv('dataset/gofood_dataset.csv')

print(df["merchant_area"].drop_duplicates())

# # =========================
# # 1. DIM MERCHANT
# # =========================
# merchant = df[["merchant_name", "merchant_area"]] \
#     .drop_duplicates() \
#     .reset_index(drop=True)

# # buat UID (UUID)
# merchant["merchant_id"] = [str(uuid.uuid4()) for _ in range(len(merchant))]

# # =========================
# # 2. MAP merchant_id ke df
# # =========================
# df = df.merge(merchant, on=["merchant_name", "merchant_area"], how="left")

# # =========================
# # 3. DIM PRODUCT
# # =========================
# product = df[[
#     "merchant_id",
#     "category",
#     "display",
#     "product",
#     "price",
#     "discount_price",
#     "isDiscount",
#     "description"
# ]].drop_duplicates().reset_index(drop=True)

# # buat product UID
# product["product_id"] = [str(uuid.uuid4()) for _ in range(len(product))]

# # =========================
# # OUTPUT
# # =========================
# print("=== MERCHANT ===")
# print(merchant.head())

# print("\n=== PRODUCT ===")
# print(product.head())



#