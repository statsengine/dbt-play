import numpy as np
import pandas as pd
from lightfm import LightFM
from lightfm.data import Dataset
from lightfm.evaluation import precision_at_k, recall_at_k
from sklearn.preprocessing import OneHotEncoder, MinMaxScaler

def model(dbt, session):
    # Step 1: Load Data
    customers_df = dbt.ref('stg_customers').to_pandas()
    interactions_df = dbt.ref('stg_orders').to_pandas()

    # Step 2: Preprocess User Features
    encoder = OneHotEncoder(sparse_output=False, handle_unknown='ignore')
    categorical_features = encoder.fit_transform(customers_df[['sex', 'city']])
    scaler = MinMaxScaler()
    numerical_features = scaler.fit_transform(customers_df[['age']])
    user_features_array = np.hstack([numerical_features, categorical_features])
    user_features_columns = ['age_scaled'] + encoder.get_feature_names_out(['sex', 'city']).tolist()
    user_features_df = pd.DataFrame(user_features_array, columns=user_features_columns)
    user_features_df['user_id'] = customers_df['user_id']

    # Step 3: Binarize Interactions
    interactions_df['interaction'] = interactions_df['total_quantity'].apply(lambda x: 1 if x > 0 else 0)
    interactions_df = interactions_df[interactions_df['interaction'] == 1]

    # Step 4: Prepare Data for LightFM
    dataset = Dataset()
    dataset.fit(
        users=customers_df['user_id'].unique(),
        items=interactions_df['product_id'].unique(),
        user_features=user_features_columns
    )
    interactions, _ = dataset.build_interactions(
        [(row['user_id'], row['product_id']) for idx, row in interactions_df.iterrows()]
    )
    user_features_list = [(row['user_id'], {feature: row[feature] for feature in user_features_columns if row[feature] != 0}) for idx, row in user_features_df.iterrows()]
    user_features_matrix = dataset.build_user_features(user_features_list, normalize=False)

    # Step 5: Train LightFM Model
    model = LightFM(loss='warp', random_state=42)
    model.fit(interactions, user_features=user_features_matrix, epochs=10, num_threads=4)

    # Step 6: Evaluate LightFM Model
    precision = precision_at_k(model, interactions, user_features=user_features_matrix, k=5).mean()
    recall = recall_at_k(model, interactions, user_features=user_features_matrix, k=5).mean()
    dbt.log(f'Precision@5: {precision:.4f}')
    dbt.log(f'Recall@5: {recall:.4f}')

    # Step 7: Generate Recommendations
    sample_user_id = customers_df['user_id'].iloc[0]
    n_users, n_items = dataset.interactions_shape()
    user_internal_id = dataset.mapping()[0].get(sample_user_id)
    scores = model.predict(user_ids=user_internal_id, item_ids=np.arange(n_items), user_features=user_features_matrix)
    top_items = np.argsort(-scores)[:5]
    item_id_reverse_map = {v: k for k, v in dataset.mapping()[2].items()}
    recommended_product_ids = [item_id_reverse_map[item] for item in top_items]

    # Log recommendations
    dbt.log(f"Recommended products for user {sample_user_id}: {recommended_product_ids}")

    # Return a DataFrame with recommendations (if needed)
    recommendations_df = pd.DataFrame({
        'user_id': [sample_user_id] * len(recommended_product_ids),
        'recommended_product_id': recommended_product_ids
    })

    return recommendations_df