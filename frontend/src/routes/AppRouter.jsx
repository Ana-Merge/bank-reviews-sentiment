import { createBrowserRouter, Navigate } from "react-router-dom";
import { lazy, Suspense } from "react";
import { LoadingSpinner } from "../components";
import App from "../App";
import NotFoundPage from "../pages/NotFoundPage";
import { MyDashboardsPage } from "../pages";

const ProductPage = lazy(() => import("../pages/ProductPage/ProductPage"));
const DashboardPage = lazy(() => import("../pages/DashboardPage/DashboardPage"));

const AppRouter = createBrowserRouter([
  {
    path: "/",
    element: <App />,
    errorElement: <NotFoundPage />,
    children: [
      {
        index: true,
        element: <Navigate to="/cards" replace />,
      },
      {
        path: "cards",
        element: (
          <Suspense fallback={<LoadingSpinner />}>
            <ProductPage />
          </Suspense>
        ),
      },
      {
        path: "deposits",
        element: (
          <Suspense fallback={<LoadingSpinner />}>
            <ProductPage />
          </Suspense>
        ),
      },
      {
        path: "credits",
        element: (
          <Suspense fallback={<LoadingSpinner />}>
            <ProductPage />
          </Suspense>
        ),
      },
      {
        path: "mortgage",
        element: (
          <Suspense fallback={<LoadingSpinner />}>
            <ProductPage />
          </Suspense>
        ),
      },
      {
        path: "investments",
        element: (
          <Suspense fallback={<LoadingSpinner />}>
            <ProductPage />
          </Suspense>
        ),
      },
      {
        path: "services",
        element: (
          <Suspense fallback={<LoadingSpinner />}>
            <ProductPage />
          </Suspense>
        ),
      },
      {
        path: "my-dashboards",
        element: (
          <Suspense fallback={<LoadingSpinner />}>
            <MyDashboardsPage />
          </Suspense>
        ),
      },
      {
        path: "dashboard/:pageId",
        element: (
          <Suspense fallback={<LoadingSpinner />}>
            <DashboardPage />
          </Suspense>
        ),
      },
      {
        path: "*",
        element: <NotFoundPage />,
      },
    ],
  },
]);

export default AppRouter;