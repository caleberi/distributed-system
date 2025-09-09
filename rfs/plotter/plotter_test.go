package plotter

import (
	"image/color"
	"math/rand/v2"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"gonum.org/v1/plot/plotter"
	"gonum.org/v1/plot/text"
	"gonum.org/v1/plot/vg"
)

func TestGraphConfig(t *testing.T) {
	config := &GraphConfig{
		Title: &GraphTitle{
			Text: "Latency Over Time",
			Style: &text.Style{
				Color: color.RGBA{R: 241, G: 160, B: 43, A: 8},
			},
		},
		BackgroundColor: color.Black,
	}
	graph := NewGraph(config)
	assert.NotNil(t, graph.Plot)
	assert.Equal(t, graph.Plot.BackgroundColor, color.Black)
	assert.Equal(t, graph.Plot.Title.Text, "Latency Over Time")
}

func TestGraph(t *testing.T) {
	testcases := []struct {
		name        string
		graphConfig *GraphConfig
		setupGraph  func(t *testing.T, config *GraphConfig) *Graph
		buildGraph  func(t *testing.T, graph *Graph) *Graph
		saveGraph   func(t *testing.T, graph *Graph)
	}{
		{

			name: "Build_Random_Point_With_Grid",
			graphConfig: &GraphConfig{
				Title: &GraphTitle{
					Text: "Random Points",
				},
				BackgroundColor: color.White,
			},
			setupGraph: func(t *testing.T, config *GraphConfig) *Graph {
				assert.NotNil(t, config)
				graph := NewGraph(config)
				assert.Equal(t, graph.Plot.BackgroundColor, color.White)
				assert.Equal(t, graph.Plot.Title.Text, "Random Points")
				return graph
			},
			buildGraph: func(t *testing.T, graph *Graph) *Graph {
				assert.NotNil(t, graph)
				points := randomPoints(6)
				assert.NotNil(t, points)

				assert.NoError(
					t, graph.InsertLinePoints(GraphLabelPoint{
						Label:  "Random",
						Points: points,
					}))

				grid := plotter.NewGrid()
				assert.NotNil(t, grid)
				grid.Vertical.Width = vg.Points(0.1)
				grid.Vertical.Color = color.RGBA{R: 38, G: 8, B: 18, A: 1}
				grid.Vertical.Width = vg.Points(0.1)
				grid.Horizontal.Color = color.RGBA{R: 38, G: 8, B: 18, A: 1}
				graph.InsertComponent(grid)

				return graph
			},
			saveGraph: func(t *testing.T, graph *Graph) {
				assert.NotNil(t, graph)
				err := graph.Save(4*vg.Inch, 4*vg.Inch, "random_points.png")
				assert.NoError(t, err)
				_, err = os.Stat("random_points.png")
				assert.NoError(t, err)
				err = os.Remove("random_points.png")
				assert.NoError(t, err)
			},
		},
		{
			name: "Build_Styled_Points",
			graphConfig: &GraphConfig{
				Title: &GraphTitle{
					Text: "Random Styled Points",
				},
				BackgroundColor: color.White,
			},
			setupGraph: func(t *testing.T, config *GraphConfig) *Graph {
				assert.NotNil(t, config)
				graph := NewGraph(config)
				return graph
			},
			buildGraph: func(t *testing.T, graph *Graph) *Graph {
				assert.NotNil(t, graph)
				scatteredData := randomPoints(10)
				assert.NotNil(t, scatteredData)
				graph.InsertComponent(plotter.NewGrid())
				scattered, err := plotter.NewScatter(scatteredData)
				assert.NoError(t, err)
				assert.NotNil(t, scattered)
				scattered.GlyphStyle.Color = color.RGBA{R: 255, B: 128, A: 255}
				graph.InsertComponent(scattered)
				lineData := randomPoints(10)
				assert.NotNil(t, lineData)
				line, err := plotter.NewLine(lineData)
				assert.NoError(t, err)
				assert.NotNil(t, line)
				line.LineStyle.Width = vg.Points(1.0)
				line.LineStyle.Dashes = []vg.Length{vg.Points(5), vg.Points(5)}
				line.LineStyle.Color = color.RGBA{B: 255, A: 255}

				graph.InsertComponent(scattered, line)
				graph.Plot.Legend.Add("scatter", scattered)
				graph.Plot.Legend.Add("line", line)

				return graph
			},
			saveGraph: func(t *testing.T, graph *Graph) {
				assert.NotNil(t, graph)
				err := graph.Save(4*vg.Inch, 4*vg.Inch, "points.png")
				assert.NoError(t, err)
				// _, err = os.Stat("points.png")
				// assert.NoError(t, err)
				// err = os.Remove("points.png")
				// assert.NoError(t, err)
			},
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			graph := tc.setupGraph(t, tc.graphConfig)
			graph = tc.buildGraph(t, graph)
			tc.saveGraph(t, graph)
		})
	}
}

func randomPoints(n int) plotter.XYs {
	pts := make(plotter.XYs, n)
	for i := range pts {
		if i == 0 {
			pts[i].X = rand.Float64()
		} else {
			pts[i].X = pts[i-1].X + rand.Float64()
		}
		pts[i].Y = pts[i].X + 10*rand.Float64()
	}
	return pts
}
