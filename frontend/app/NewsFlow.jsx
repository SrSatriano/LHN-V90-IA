import React, { useState, useEffect } from 'react';
import { useGlobalWebSocket } from './WebSocketContext';

const NewsFlow = () => {
  const [news, setNews] = useState([]);
  const [filter, setFilter] = useState('ALL'); // ALL, PANIC, EUPHORIA
  const { isConnected, latestMessage } = useGlobalWebSocket();

  // Dicionários básicos para o filtro no Frontend
  const panicWords = ['queda', 'tensão', 'guerra', 'hacker', 'roubo', 'sec', 'processo', 'derretimento', 'colapso', 'fraude', 'pânico', 'crash', 'dump', 'liquidação', 'falência'];
  const euphoriaWords = ['alta', 'lua', 'recorde', 'adoção', 'parceria', 'aprovado', 'etf', 'halving', 'disparo', 'touro', 'otimismo', 'pump', 'bull', 'moon', 'ath'];

  useEffect(() => {
    if (latestMessage?.news && Array.isArray(latestMessage.news)) {
      setNews(latestMessage.news);
    }
  }, [latestMessage]);

  // Lógica de Filtro
  const filteredNews = news.filter((item) => {
    const titleLower = item.title.toLowerCase();
    if (filter === 'PANIC') {
      return panicWords.some(word => titleLower.includes(word));
    }
    if (filter === 'EUPHORIA') {
      return euphoriaWords.some(word => titleLower.includes(word));
    }
    return true;
  });

  return (
    <div style={{ backgroundColor: '#131722', color: '#D1D4DC', height: '100%', display: 'flex', flexDirection: 'column', borderRadius: '8px', border: '1px solid #2A2E39', overflow: 'hidden' }}>
      
      {/* Cabeçalho da Aba */}
      <div style={{ padding: '16px', backgroundColor: '#1E222D', borderBottom: '1px solid #2A2E39', display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: '12px' }}>
          <h2 style={{ margin: 0, fontSize: '18px', fontWeight: '600' }}>News Flow (Ao Vivo)</h2>
          {/* Indicador de Status WebSocket */}
          <span style={{ display: 'flex', alignItems: 'center', gap: '6px', fontSize: '12px', color: isConnected ? '#089981' : '#F23645' }}>
            <span style={{ width: '8px', height: '8px', borderRadius: '50%', backgroundColor: isConnected ? '#089981' : '#F23645', display: 'inline-block', boxShadow: isConnected ? '0 0 8px #089981' : 'none' }}></span>
            {isConnected ? 'Sincronizado' : 'Desconectado'}
          </span>
        </div>

        {/* Botões de Filtro estilo TradingView */}
        <div style={{ display: 'flex', gap: '8px' }}>
          {['ALL', 'EUPHORIA', 'PANIC'].map((f) => (
            <button
              key={f}
              onClick={() => setFilter(f)}
              style={{
                backgroundColor: filter === f ? '#2962FF' : 'transparent',
                color: filter === f ? '#FFF' : '#8B98A5',
                border: `1px solid ${filter === f ? '#2962FF' : '#2A2E39'}`,
                padding: '4px 12px',
                borderRadius: '4px',
                cursor: 'pointer',
                fontSize: '12px',
                transition: 'all 0.2s'
              }}
            >
              {f === 'ALL' ? 'Todas' : f === 'EUPHORIA' ? '🚀 Alta Relevância' : '⚠️ Alertas Críticos'}
            </button>
          ))}
        </div>
      </div>

      {/* Lista de Notícias (Scrollable) */}
      <div style={{ flex: 1, overflowY: 'auto', padding: '12px', display: 'flex', flexDirection: 'column', gap: '8px' }}>
        {filteredNews.length === 0 ? (
          <div style={{ textAlign: 'center', color: '#8B98A5', marginTop: '20px' }}>Nenhuma notícia encontrada para este filtro.</div>
        ) : (
          filteredNews.map((item, index) => (
            <div 
              key={index} 
              style={{ 
                padding: '12px', 
                backgroundColor: '#1E222D', 
                borderRadius: '6px', 
                borderLeft: `3px solid ${euphoriaWords.some(w => item.title.toLowerCase().includes(w)) ? '#089981' : panicWords.some(w => item.title.toLowerCase().includes(w)) ? '#F23645' : '#2962FF'}`,
                display: 'flex',
                flexDirection: 'column',
                gap: '6px'
              }}
            >
              <div style={{ display: 'flex', justifyContent: 'space-between', fontSize: '11px', color: '#8B98A5' }}>
                <span>{item.provider}</span>
                <span>{item.time}</span>
              </div>
              <div style={{ fontSize: '14px', fontWeight: '500', lineHeight: '1.4' }}>
                {item.title}
              </div>
            </div>
          ))
        )}
      </div>
    </div>
  );
};

export default NewsFlow;