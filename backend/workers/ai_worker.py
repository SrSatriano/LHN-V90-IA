import os
import numpy as np

# Padrão Warm Singleton: O worker filho aloca em memória global e a mantém entre requisições
_modelos_locais = {}

def get_model(weights_path):
    global _modelos_locais
    
    if weights_path not in _modelos_locais:
        # Import atrasado para não carregar o framework se o worker for usado para outra coisa
        import tensorflow as tf
        from keras.models import load_model

        # Tentativa de inicializar sem travar o worker ou a GPU
        os.environ['TF_CPP_MIN_LOG_LEVEL'] = '3'
        tf.get_logger().setLevel('ERROR')

        _modelos_locais[weights_path] = load_model(weights_path, compile=False)
    
    return _modelos_locais[weights_path]

def predict_isolated(weights_path_sn_t, weights_path_lat_t, weights_path_sn_r, weights_path_lat_r, X_numpy, idx_lat, idx_snp, use_mc_dropout=False, mc_samples=5):
    """
    Realiza predição limpa isolada. Recebe tensores em formato NumPy e responde NumPy Array.
    A Fronteira Assíncrona.
    """
    m_sn_t = get_model(weights_path_sn_t) if weights_path_sn_t else None
    m_sn_r = get_model(weights_path_sn_r) if weights_path_sn_r else m_sn_t
    m_lat_t = get_model(weights_path_lat_t) if weights_path_lat_t else None
    m_lat_r = get_model(weights_path_lat_r) if weights_path_lat_r else m_lat_t

    n_batch = len(X_numpy)
    
    if use_mc_dropout:
        mc_preds_t = []
        mc_preds_r = []
        for _ in range(mc_samples):
            layer_t = np.zeros((n_batch, 1))
            layer_r = np.zeros((n_batch, 1))
            if idx_lat and m_lat_t:
                layer_t[idx_lat] = m_lat_t(X_numpy[idx_lat], training=True).numpy()
                layer_r[idx_lat] = m_lat_r(X_numpy[idx_lat], training=True).numpy()
            if idx_snp and m_sn_t:
                layer_t[idx_snp] = m_sn_t(X_numpy[idx_snp], training=True).numpy()
                layer_r[idx_snp] = m_sn_r(X_numpy[idx_snp], training=True).numpy()
            mc_preds_t.append(layer_t)
            mc_preds_r.append(layer_r)
        
        _pred_t = np.mean(mc_preds_t, axis=0)
        _pred_r = np.mean(mc_preds_r, axis=0)
        _pred_std = np.std(mc_preds_t, axis=0)
        return _pred_t, _pred_r, _pred_std
    else:
        _pred_t = np.zeros((n_batch, 1))
        _pred_r = np.zeros((n_batch, 1))
        if idx_lat and m_lat_t:
            _pred_t[idx_lat] = m_lat_t.predict(X_numpy[idx_lat], verbose=0)
            _pred_r[idx_lat] = m_lat_r.predict(X_numpy[idx_lat], verbose=0)
        if idx_snp and m_sn_t:
            _pred_t[idx_snp] = m_sn_t.predict(X_numpy[idx_snp], verbose=0)
            _pred_r[idx_snp] = m_sn_r.predict(X_numpy[idx_snp], verbose=0)
        return _pred_t, _pred_r, None

def train_isolated(weights_path, X_numpy, y_numpy, batch_size=32, epochs=5):
    """
    Treinamento Isolado: Destrói o grafo após a conclusão, limpando a vazada da memória.
    """
    import gc
    import keras.backend as K
    
    model = get_model(weights_path)
    if model:
        # Compila localmente para o treino rápido
        model.compile(optimizer='adam', loss='binary_crossentropy', metrics=['accuracy'])
        model.fit(X_numpy, y_numpy, batch_size=batch_size, epochs=epochs, verbose=0)
        model.save(weights_path) # Atualiza disco
        
        # O Clear Session essencial para a retenção pesada (Treino)
        K.clear_session()
        _modelos_locais.pop(weights_path, None)  # Remove do cache
        gc.collect()

    return True

def predict_guardiao_isolated(weights_path, X_numpy):
    """
    Predição isolada para a rede Guardião (ProcessPool).
    """
    model = get_model(weights_path) if weights_path else None
    if model:
        pred = model.predict(X_numpy, verbose=0)
        return pred[0]
    return [0.0, 1.0, 0.0]  # Array dummy de fallback (Q-Values)
