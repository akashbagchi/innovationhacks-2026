import { useState, useRef, useEffect } from 'react'
import { AnimatePresence, motion } from 'framer-motion'
import { X, Send } from 'lucide-react'
import ReactMarkdown from 'react-markdown'
import remarkGfm from 'remark-gfm'
import { ProvenanceChip } from './ProvenanceChip'
import { sendChatMessage } from '../lib/api'
import type { ChatMessage } from '../types/chat'

interface ChatPanelProps {
    open: boolean
    onClose: () => void
}

const mono: React.CSSProperties = { fontFamily: "'IBM Plex Mono', monospace" }

export function ChatPanel({ open, onClose }: ChatPanelProps) {
    const [messages, setMessages] = useState<ChatMessage[]>([])
    const [inputValue, setInputValue] = useState('')
    const [loading, setLoading] = useState(false)
    const scrollRef = useRef<HTMLDivElement>(null)
    const inputRef = useRef<HTMLInputElement>(null)

    useEffect(() => {
        if (open) inputRef.current?.focus()
    }, [open])

    useEffect(() => {
        if (scrollRef.current) {
            scrollRef.current.scrollTop = scrollRef.current.scrollHeight
        }
    }, [messages, loading])

    async function handleSend() {
        const text = inputValue.trim()
        if (!text || loading) return

        const userMsg: ChatMessage = {
            id: crypto.randomUUID(),
            role: 'user',
            content: text,
            timestamp: new Date(),
        }
        setMessages(prev => [...prev, userMsg])
        setInputValue('')
        setLoading(true)

        try {
            const history = messages.map(m => ({ role: m.role, content: m.content }))
            const { reply, sources } = await sendChatMessage(text, history)
            const assistantMsg: ChatMessage = {
                id: crypto.randomUUID(),
                role: 'assistant',
                content: reply,
                timestamp: new Date(),
                sources: sources.length > 0 ? sources : undefined,
            }
            setMessages(prev => [...prev, assistantMsg])
        } catch {
            setMessages(prev => [...prev, {
                id: crypto.randomUUID(),
                role: 'assistant',
                content: 'Unable to reach the policy assistant. Check that the API is running.',
                timestamp: new Date(),
            }])
        } finally {
            setLoading(false)
        }
    }

    function handleKeyDown(e: React.KeyboardEvent<HTMLInputElement>) {
        if (e.key === 'Enter' && !e.shiftKey) {
            e.preventDefault()
            handleSend()
        }
    }

    return (
        <AnimatePresence>
            {open && (
                <motion.div
                    initial={{ x: 380, opacity: 0 }}
                    animate={{ x: 0, opacity: 1 }}
                    exit={{ x: 380, opacity: 0 }}
                    transition={{ duration: 0.18, ease: 'easeOut' }}
                    style={{
                        position: 'fixed',
                        top: 0,
                        right: 0,
                        width: '480px',
                        height: '100vh',
                        background: '#FFFFFF',
                        borderLeft: '1px solid #D8D4CC',
                        display: 'flex',
                        flexDirection: 'column',
                        zIndex: 50,
                    }}
                >
                    {/* Header */}
                    <div style={{
                        padding: '14px 16px',
                        borderBottom: '1px solid #D8D4CC',
                        display: 'flex',
                        alignItems: 'center',
                        justifyContent: 'space-between',
                        flexShrink: 0,
                    }}>
                        <div>
                            <p style={{ ...mono, fontSize: '9px', letterSpacing: '0.2em', textTransform: 'uppercase', color: '#918D88', marginBottom: '2px' }}>
                                AI
                            </p>
                            <p style={{ fontSize: '13px', fontWeight: 700, color: '#131210', lineHeight: 1 }}>
                                Policy Assistant
                            </p>
                        </div>
                        <button
                            onClick={onClose}
                            style={{ background: 'none', border: 'none', cursor: 'pointer', color: '#918D88', padding: '4px', borderRadius: '2px', display: 'flex', alignItems: 'center' }}
                            onMouseEnter={e => (e.currentTarget.style.color = '#131210')}
                            onMouseLeave={e => (e.currentTarget.style.color = '#918D88')}
                        >
                            <X size={14} />
                        </button>
                    </div>

                    {/* Messages */}
                    <div
                        ref={scrollRef}
                        style={{ flex: 1, overflowY: 'auto', padding: '16px', display: 'flex', flexDirection: 'column', gap: '12px' }}
                    >
                        {messages.length === 0 && (
                            <div style={{ margin: 'auto', textAlign: 'center', padding: '32px 16px' }}>
                                <p style={{ ...mono, fontSize: '9px', letterSpacing: '0.2em', textTransform: 'uppercase', color: '#D8D4CC', marginBottom: '8px' }}>
                                    Ready
                                </p>
                                <p style={{ fontSize: '12px', color: '#918D88', lineHeight: 1.6 }}>
                                    Ask about PA criteria, step therapy requirements, payer comparisons, or recent policy changes.
                                </p>
                            </div>
                        )}

                        {messages.map(msg => (
                            <motion.div
                                key={msg.id}
                                initial={{ opacity: 0, y: 6 }}
                                animate={{ opacity: 1, y: 0 }}
                                transition={{ duration: 0.12 }}
                                style={{ display: 'flex', flexDirection: 'column', alignItems: msg.role === 'user' ? 'flex-end' : 'flex-start', gap: '6px' }}
                            >
                                <div style={{
                                    maxWidth: '88%',
                                    padding: '9px 12px',
                                    borderRadius: '2px',
                                    fontSize: '12px',
                                    lineHeight: 1.65,
                                    color: '#131210',
                                    background: msg.role === 'user' ? '#EAF3FC' : '#F0EFEB',
                                    border: `1px solid ${msg.role === 'user' ? 'rgba(145,191,235,0.5)' : '#D8D4CC'}`,
                                    whiteSpace: msg.role === 'user' ? 'pre-wrap' : undefined,
                                }}>
                                    {msg.role === 'user' ? msg.content : (
                                        <ReactMarkdown
                                            remarkPlugins={[remarkGfm]}
                                            components={{
                                                p: ({ children }) => <p style={{ margin: '0 0 6px 0' }}>{children}</p>,
                                                h1: ({ children }) => <p style={{ fontWeight: 700, fontSize: '13px', margin: '8px 0 4px 0', color: '#131210' }}>{children}</p>,
                                                h2: ({ children }) => <p style={{ fontWeight: 700, fontSize: '12px', margin: '8px 0 4px 0', color: '#131210' }}>{children}</p>,
                                                h3: ({ children }) => <p style={{ fontWeight: 600, fontSize: '12px', margin: '6px 0 3px 0', color: '#131210' }}>{children}</p>,
                                                strong: ({ children }) => <strong style={{ fontWeight: 600, color: '#131210' }}>{children}</strong>,
                                                ul: ({ children }) => <ul style={{ margin: '4px 0', paddingLeft: '16px' }}>{children}</ul>,
                                                ol: ({ children }) => <ol style={{ margin: '4px 0', paddingLeft: '16px' }}>{children}</ol>,
                                                li: ({ children }) => <li style={{ margin: '2px 0' }}>{children}</li>,
                                                hr: () => <hr style={{ border: 'none', borderTop: '1px solid #D8D4CC', margin: '8px 0' }} />,
                                                blockquote: ({ children }) => (
                                                    <blockquote style={{ borderLeft: '2px solid #91bfeb', paddingLeft: '8px', margin: '6px 0', color: '#4A4845' }}>
                                                        {children}
                                                    </blockquote>
                                                ),
                                                code: ({ children }) => (
                                                    <code style={{ fontFamily: "'IBM Plex Mono', monospace", fontSize: '10px', background: '#FFFFFF', border: '1px solid #D8D4CC', borderRadius: '2px', padding: '1px 4px' }}>
                                                        {children}
                                                    </code>
                                                ),
                                                table: ({ children }) => (
                                                    <div style={{ overflowX: 'auto', margin: '6px 0' }}>
                                                        <table style={{ borderCollapse: 'collapse', width: '100%', fontSize: '11px' }}>{children}</table>
                                                    </div>
                                                ),
                                                th: ({ children }) => (
                                                    <th style={{ padding: '4px 8px', background: '#FFFFFF', border: '1px solid #D8D4CC', fontWeight: 600, textAlign: 'left', fontFamily: "'IBM Plex Mono', monospace", fontSize: '10px', letterSpacing: '0.05em' }}>
                                                        {children}
                                                    </th>
                                                ),
                                                td: ({ children }) => (
                                                    <td style={{ padding: '4px 8px', border: '1px solid #EBEBEB', verticalAlign: 'top' }}>{children}</td>
                                                ),
                                            }}
                                        >
                                            {msg.content}
                                        </ReactMarkdown>
                                    )}
                                </div>
                                {msg.sources && msg.sources.length > 0 && (
                                    <div style={{ display: 'flex', flexWrap: 'wrap', gap: '4px', maxWidth: '88%' }}>
                                        {msg.sources.map(src => (
                                            <ProvenanceChip
                                                key={src.mongo_id}
                                                payer={src.payer}
                                                policyTitle={src.policy_title}
                                            />
                                        ))}
                                    </div>
                                )}
                            </motion.div>
                        ))}

                        {loading && (
                            <motion.div
                                initial={{ opacity: 0 }}
                                animate={{ opacity: 1 }}
                                style={{ display: 'flex', alignItems: 'flex-start' }}
                            >
                                <div style={{
                                    padding: '9px 12px',
                                    borderRadius: '2px',
                                    background: '#F0EFEB',
                                    border: '1px solid #D8D4CC',
                                }}>
                                    <LoadingDots />
                                </div>
                            </motion.div>
                        )}
                    </div>

                    {/* Input */}
                    <div style={{ padding: '12px', borderTop: '1px solid #D8D4CC', flexShrink: 0 }}>
                        <div style={{ display: 'flex', gap: '8px', alignItems: 'center' }}>
                            <input
                                ref={inputRef}
                                value={inputValue}
                                onChange={e => setInputValue(e.target.value)}
                                onKeyDown={handleKeyDown}
                                placeholder="Ask about a drug, payer, or policy change…"
                                disabled={loading}
                                style={{
                                    flex: 1,
                                    background: '#FFFFFF',
                                    border: '1px solid #D8D4CC',
                                    borderRadius: '2px',
                                    color: '#131210',
                                    fontFamily: 'IBM Plex Sans, system-ui',
                                    fontSize: '12px',
                                    padding: '8px 10px',
                                    outline: 'none',
                                    opacity: loading ? 0.6 : 1,
                                }}
                                onFocus={e => (e.currentTarget.style.borderColor = '#91bfeb')}
                                onBlur={e => (e.currentTarget.style.borderColor = '#D8D4CC')}
                            />
                            <button
                                onClick={handleSend}
                                disabled={loading || !inputValue.trim()}
                                style={{
                                    background: inputValue.trim() && !loading ? '#131210' : '#F0EFEB',
                                    border: `1px solid ${inputValue.trim() && !loading ? '#131210' : '#D8D4CC'}`,
                                    borderRadius: '2px',
                                    color: inputValue.trim() && !loading ? '#FFFFFF' : '#918D88',
                                    cursor: inputValue.trim() && !loading ? 'pointer' : 'default',
                                    padding: '8px 10px',
                                    display: 'flex',
                                    alignItems: 'center',
                                    transition: 'all 0.1s',
                                    flexShrink: 0,
                                }}
                            >
                                <Send size={13} />
                            </button>
                        </div>
                    </div>
                </motion.div>
            )}
        </AnimatePresence>
    )
}

function LoadingDots() {
    return (
        <div style={{ display: 'flex', gap: '4px', alignItems: 'center', height: '16px' }}>
            {[0, 1, 2].map(i => (
                <motion.span
                    key={i}
                    animate={{ opacity: [0.3, 1, 0.3] }}
                    transition={{ duration: 1, repeat: Infinity, delay: i * 0.2 }}
                    style={{ width: '4px', height: '4px', borderRadius: '50%', background: '#918D88', display: 'block' }}
                />
            ))}
        </div>
    )
}
