import { LineChart, Line, ResponsiveContainer } from 'recharts'

interface SparkLineProps {
  data: number[]
  color: string
  width?: number
  height?: number
}

export function SparkLine({ data, color, width = 64, height = 32 }: SparkLineProps) {
  const chartData = data.map((v, i) => ({ i, v }))
  return (
    <div style={{ width, height, flexShrink: 0 }}>
      <ResponsiveContainer width="100%" height="100%">
        <LineChart data={chartData}>
          <Line
            type="monotone"
            dataKey="v"
            stroke={color}
            strokeWidth={1.5}
            dot={false}
            activeDot={false}
            isAnimationActive={false}
          />
        </LineChart>
      </ResponsiveContainer>
    </div>
  )
}
