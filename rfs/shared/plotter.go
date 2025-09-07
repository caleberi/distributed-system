package shared

import (
	"image/color"

	"gonum.org/v1/plot"
	"gonum.org/v1/plot/plotter"
	"gonum.org/v1/plot/plotutil"
	"gonum.org/v1/plot/text"
	"gonum.org/v1/plot/vg"
)

const (
	ColorRed        = "red"
	ColorBlue       = "blue"
	ColorGreen      = "green"
	ColorOrange     = "orange"
	ColorPurple     = "purple"
	ColorBrown      = "brown"
	ColorPink       = "pink"
	ColorGray       = "gray"
	ColorOlive      = "olive"
	ColorCyan       = "cyan"
	ColorPeach      = "peach"
	ColorLightGreen = "Light Green"
)

var ColorsPaletteMap = map[string]color.RGBA{
	ColorRed:        {R: 214, G: 39, B: 40, A: 255},   // Red
	ColorBlue:       {R: 31, G: 119, B: 180, A: 255},  // Blue
	ColorGreen:      {R: 44, G: 160, B: 44, A: 255},   // Green
	ColorOrange:     {R: 255, G: 127, B: 14, A: 255},  // Orange
	ColorPurple:     {R: 148, G: 103, B: 189, A: 255}, // Purple
	ColorBrown:      {R: 140, G: 86, B: 75, A: 255},   // Brown
	ColorPink:       {R: 227, G: 119, B: 194, A: 255}, // Pink
	ColorGray:       {R: 127, G: 127, B: 127, A: 255}, // Gray
	ColorOlive:      {R: 188, G: 189, B: 34, A: 255},  // Olive
	ColorCyan:       {R: 23, G: 190, B: 207, A: 255},  // Cyan
	ColorPeach:      {R: 255, G: 187, B: 120, A: 255}, // Peach
	ColorLightGreen: {R: 152, G: 223, B: 138, A: 255}, // Light Green
}

type GraphTitle struct {
	Text    string
	Padding vg.Length
	Style   *text.Style
}

type GraphConfig struct {
	X, Y            *plot.Axis
	Legend          *plot.Legend
	Title           *GraphTitle
	BackgroundColor color.Color
	TextHandler     text.Handler
}

type GraphLabelPoint struct {
	Label  string
	Points plotter.XYer
}

type Graph struct {
	Plot *plot.Plot
}

func NewGraph(config *GraphConfig) *Graph {
	p := plot.New()

	if config == nil {
		return &Graph{Plot: p}
	}

	if config.Title != nil {
		p.Title.Text = config.Title.Text
		p.Title.Padding = config.Title.Padding
		if config.Title.Style != nil {
			p.Title.TextStyle = *config.Title.Style
		}
	}

	if config.X != nil {
		p.X = *config.X
	}
	if config.Y != nil {
		p.Y = *config.Y
	}
	if config.Legend != nil {
		p.Legend = *config.Legend
	}
	if config.BackgroundColor != color.Color(nil) {
		p.BackgroundColor = config.BackgroundColor
	}

	if config.TextHandler != text.Handler(nil) {
		p.TextHandler = config.TextHandler
	}
	return &Graph{
		Plot: p,
	}
}

func (g *Graph) GetColor(name string) color.RGBA {
	if c, ok := ColorsPaletteMap[name]; ok {
		return c
	}
	return ColorsPaletteMap[ColorBlue]
}

func (g *Graph) Save(width, height vg.Length, filename string) error {
	return g.Plot.Save(width, height, filename)
}

func (g *Graph) InsertComponent(components ...plot.Plotter) {
	g.Plot.Add(components...)
}

func (g *Graph) InsertLinePoints(points ...GraphLabelPoint) error {
	for _, lp := range points {
		if err := plotutil.AddLinePoints(g.Plot, lp.Label, lp.Points); err != nil {
			return err
		}

	}
	return nil
}
