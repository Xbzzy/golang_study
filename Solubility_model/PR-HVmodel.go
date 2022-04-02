package Solubility_model

import "math"

const (
	R = 8.3147 // 通用气体常数,单位Mpa.cm3/(mol.K);
)

//关于偏心因子的函数,w 偏心因子;
func m(w float64) float64 {
	return 0.37646 + 1.5426*w - 0.26992*w*w
}

func a1i(Tri float64, w float64) float64 {
	return math.Pow(1+m(w)*(1-math.Pow(Tri, 0.5)), 2)
}

// GE 不限定压力的条件下,Gibbs自由,T 为当前体系温度;w 偏心因子; xi 为液相中组分i的摩尔分数,
// Tc 为i组分的临界温度, pc 为i组分的临界压力, k 为组分ij之间的相互作用系数, a1 为组分ij之间的非随机参数;
func GE(n int, T float64, x []float64, w float64, Tc []float64, pc []float64, kij [][]float64, a1 [][]float64) float64 {
	var Ge float64
	var GeUp float64
	var GeDown float64
	for i := 0; i < n; i++ {
		for j := 0; j < n; j++ {
			bI := bi(Tc[i], pc[i])
			bJ := bi(Tc[j], pc[j])
			// gji,gii 分别表示不同和相同分子间作用的玻尔兹曼因子;
			gii := -1 * c0() * ai(T, Tc[i], pc[i], w) / bi(Tc[i], pc[i])
			gjj := -1 * c0() * ai(T, Tc[j], pc[j], w) / bi(Tc[j], pc[j])
			gjiUp := -2 * bI * bJ * math.Pow(gii*gjj, 0.5) * (1 - kij[i][j])
			gjiDown := bI + bJ
			gji := gjiUp / gjiDown
			Cji := gji - gii
			Gji := bi(Tc[j], pc[j]) * math.Pow(math.E, -1*a1[j][i]*Cji/(R*T))
			GeUp += x[j] * Cji * Gji
		}
		for k := 0; k < n; k++ {
			bI := bi(Tc[i], pc[i])
			bK := bi(Tc[k], pc[k])
			gii := -1 * c0() * ai(T, Tc[i], pc[i], w) / bi(Tc[i], pc[i])
			gkk := -1 * c0() * ai(T, Tc[k], pc[k], w) / bi(Tc[k], pc[k])
			gkiUp := -2 * bI * bK * math.Pow(gii*gkk, 0.5) * (1 - kij[i][k])
			gkiDown := bI + bK
			gki := gkiUp / gkiDown
			Cki := gki - gii
			Gki := bi(Tc[k], pc[k]) * math.Pow(math.E, -1*a1[k][i]*Cki/(R*T))
			GeDown += x[k] * Gki
		}
		Ge += GeUp / GeDown
	}
	return 0
}

func ai(T float64, Tci float64, pci float64, w float64) float64 {
	Tri := T / Tci
	a1 := a1i(Tri, w)
	aiConst := 0.477235
	return aiConst * math.Pow(R*Tci, 2) * a1 / pci
}

func bi(Tci float64, pci float64) float64 {
	biConst := 0.077796
	return biConst * R * Tci / pci
}

//混合体系的引力常数
func am(n int, x []float64, T float64, Tc []float64, pc []float64, w float64, kij [][]float64, a1 [][]float64) float64 {
	tmpBm := bm(n, x, Tc, pc)
	var result float64
	for i := 0; i < n; i++ {
		result += x[i]*ai(T, Tc[i], pc[i], w)*a1i(T/Tc[i], w)/bi(Tc[i], pc[i]) - (GE(n, T, x, w, Tc, pc, kij, a1) / c0())
	}
	result *= tmpBm
	return result
}

//混合体系的斥力常数
func bm(n int, x []float64, Tc []float64, pc []float64) float64 {
	var result float64
	for i := 0; i < n; i++ {
		result += x[i] + bi(Tc[i], pc[i])
	}
	return result
}

//常数
func c0() float64 {
	c0left := 1 / (2 * math.Sqrt2)
	c0right := math.Log(2 + math.Sqrt2/2 - math.Sqrt2)
	return c0left * c0right
}

//systemPressure P 体系压力,MPa; T为体系温度,K; v为摩尔体积,cm3/mol;
func systemPressure(T float64, v float64, n int, x []float64, Tc []float64, pc []float64, w float64, kij [][]float64, a1 [][]float64) float64 {
	var result float64
	tmpAm := am(n, x, T, Tc, pc, w, kij, a1)
	tmpBm := bm(n, x, Tc, pc)
	resultLeft := R * T / (v - bm(n, x, Tc, pc))
	resultRight := tmpAm / (v*(v+tmpBm) + tmpBm*(v-tmpBm))
	result = resultLeft - resultRight
	return result
}

// i 当前组分; n 组分数量; T 体系温度; zm 混合物的偏差因子; vm 混合体系的摩尔体积; x 液相中组分i的摩尔分数; Tc 为i组分的临界温度,
// pc 为i组分的临界压力, kij 为组分ij之间的相互作用系数, a1 为组分ij之间的非随机参数,y 为混合物中组分i的活度系数;
func escapeCoefficient(i int, n int, T float64, zm float64, vm float64, x []float64, Tc []float64, pc []float64, w float64, y []float64) float64 {
	var power float64
	var result float64
	tmpAi := ai(T, Tc[i], pc[i], w)
	tmpBi := bi(Tc[i], pc[i])
	tmpBm := bm(n, x, Tc, pc)
	power1 := bi(Tc[i], pc[i]) * (zm - 1) / tmpBm
	power2 := math.Log(zm * (1 - (tmpBm / vm)))
	power3Left := 1 / (2 * math.Sqrt2 * R * T) * ((tmpAi / tmpBi) - (R * T * math.Log(y[i]) / c0()))
	power3Right := math.Log((vm + (math.Sqrt2+1)*tmpBm) / (vm - (math.Sqrt2-1)*tmpBm))
	power3 := power3Left * power3Right
	power = power1 - power2 - power3
	result = math.Pow(math.E, power)
	return result
}
