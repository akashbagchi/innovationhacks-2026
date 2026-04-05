export interface ChatSource {
  payer: string
  policy_title: string
  drug_id: string
  s3_key?: string
  mongo_id: string
}

export interface ChatMessage {
  id: string
  role: 'user' | 'assistant'
  content: string
  timestamp: Date
  sources?: ChatSource[]
}
